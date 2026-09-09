"""
backfill_compare_event.py — rebuild a past rainfall event into a permanent R2
prefix so a pinned /compare-style page can replay it long after the live
retention windows have closed.

Why this exists
---------------
The live /compare page is a *nowcast* tool. Everything it reads is on a short
rolling window:

  * ukv/, ukv_poly/, ukv_gauge/, ukv_area_ts/  — 72h. cleanup_old_ukv_runs()
    purges any run that has fallen out of ukv_meta.json's 72h list.
  * accum_hist/, accum_poly/, gauge_bias/      — 14 days.

So a few days after an event, the forecast half of the comparison is gone
outright and the observed half is on a clock. This script rebuilds both halves
under `compare_events/{event_id}/` — a prefix no cleanup job touches — so the
event stays interrogable permanently.

The forecast half is genuinely regenerated, not recovered: the Met Office public
S3 bucket keeps its UKV NetCDF far longer than we keep our renders (verified
back to Sep 2024), so the runs are re-downloaded and re-rendered through the
same code path fetch_ukv.py uses live. The observed half cannot be regenerated
— radar accumulations are built from frames we no longer hold — so it is
*copied* out of the live 14-day prefixes before they expire. That asymmetry is
the reason `--mode radar` is time-critical and `--mode ukv` is not.

Modes
-----
  --mode ukv    --event <id> [--runs a,b,c]  Re-render UKV runs. With --runs,
                only those runs (shard across matrix jobs); without, all of the
                event's runs. Writes a per-run manifest fragment so the job is
                resumable and shardable.
  --mode radar  --event <id>                 Copy the observed side (radar
                accumulation PNGs, area polygons, gauge bias snapshots) out of
                the live prefixes into the event prefix.
  --mode meta   --event <id>                 Assemble the per-run fragments and
                the copied radar snapshots into the single meta.json the page
                loads.
  --mode purge  --event <id> [--force]       Bin an expired event: delete its
                whole R2 prefix, its page directory and its menu card. Refuses
                to act before the event's `expires` date unless --force is
                given. Run daily by the Expire Compare Event workflow.

Each mode is idempotent — already-present objects are skipped rather than
rewritten, so a re-run after a partial failure costs Class B operations rather
than Class A ones (see CLAUDE.md's R2 budget rules).

Usage: python backfill_compare_event.py --mode ukv --event early_sep_2026
Required env: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
              R2_BUCKET_NAME, R2_PUBLIC_BASE_URL
"""
import argparse
import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np

import fetch_ukv as fu

# ── Event definitions ─────────────────────────────────────────────────────────
#
# `runs` is the inclusive 3-hourly run range. Only the synoptic 00/03/06/09/12/
# 15/18/21Z runs are used: the off-cycle hourly UKV runs only ever publish to
# T+11h (see CLAUDE.md), so they carry no useful lead time for event review.
#
# `radar` is the observed-side window, in UTC. It wants to span the valid times
# worth verifying against, not the full T+54h reach of the earliest run — steps
# that fall outside it simply have no radar frame to compare with, which the
# page already handles.
EVENTS = {
    "early_sep_2026": {
        "title": "Early September 2026",
        "subtitle": "Thu 3 Sep evening – Fri 4 Sep morning",
        "description": (
            "Every three-hourly UKV run from the start of Wednesday 2 September "
            "through to Friday 4 September 00Z, verified against radar and rain "
            "gauges across the Thursday evening into Friday morning rainfall event."
        ),
        "run_start": "20260902T0000Z",
        "run_end":   "20260904T0000Z",
        "radar_start": "202609020000",
        "radar_end":   "202609050000",
        # The rainfall event itself, inside the wider observed window. The page
        # shades this span on the timeline and opens on `focus_ts` rather than
        # at the end of the archive, so the first thing on screen is the event
        # rather than the quiet Saturday after it.
        "event_start": "202609031800",
        "event_end":   "202609040900",
        "focus_ts":    "202609032100",
        # Run the page selects first: late enough to have the event in short
        # range, early enough that it is still a forecast rather than an
        # analysis. Every other run stays one dropdown click away.
        "default_run": "20260903T1200Z",
        # This case study is deliberately temporary. On or after this date the
        # expiry workflow purges the event's R2 prefix and deletes its page, so
        # a one-off review does not sit on the bucket forever. Push the date out
        # to keep it longer; delete the key to make the event permanent.
        "expires": "2026-09-23",
        # Local artefacts removed alongside the R2 data when the event expires.
        "page_dir":  "compare-early-september",
        "menu_href": "/compare-early-september",
    },
}

EVENT_ROOT = "compare_events"

# Radar accumulation periods the compare page offers. make_accum_multi.py also
# writes a "5d" period; the page has no control for it, so copying it would be
# ~800 Class A operations spent on something nothing can display.
RADAR_PERIODS = {"1hr": 4, "3hr": 12, "6hr": 24, "12hr": 48, "24hr": 96, "48hr": 192}
RADAR_SCHEMES = ("norm", "high", "met")

_STANDARD_HOURS = (0, 3, 6, 9, 12, 15, 18, 21)


def event_key(event_id, *parts):
    return "/".join((EVENT_ROOT, event_id) + parts)


def event_runs(event):
    """Inclusive list of 3-hourly run timestamps spanning the event."""
    start = fu.parse_run_dt(event["run_start"])
    end   = fu.parse_run_dt(event["run_end"])
    runs, cur = [], start
    while cur <= end:
        if cur.hour in _STANDARD_HOURS:
            runs.append(cur.strftime("%Y%m%dT%H%MZ"))
        cur += timedelta(hours=1)
    return runs


def radar_snapshot_ts(event):
    """Every 15-minute radar snapshot timestamp in the event's observed window."""
    start = datetime.strptime(event["radar_start"], "%Y%m%d%H%M")
    end   = datetime.strptime(event["radar_end"], "%Y%m%d%H%M")
    out, cur = [], start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d%H%M"))
        cur += timedelta(minutes=15)
    return out


# ── Time labels ───────────────────────────────────────────────────────────────
# The live pipeline labels runs and steps in UK local time via
# fetch_ukv.run_label_str() / valid_label_str(), which is right for a page about
# what is happening now. An event archive is read as a record, so every label
# here is UTC stamped "GMT" — the convention UK meteorology works in, and the
# same basis as the radar snapshot timestamps, the popup chart axis and the
# timeline readout. Mixing the two would put the run dropdown an hour out from
# the slider beside it for any event inside BST, which this one is.
def event_run_label(run_ts):
    dt = fu.parse_run_dt(run_ts)
    return f"{dt.day} " + dt.strftime("%b %Y %H:%M") + " GMT"


def event_valid_label(valid_ts):
    return event_run_label(valid_ts)


def _force_rerun():
    """Same truthiness rule fetch_ukv.py uses, so a literal "false" from a
    workflow input is not read as a request to re-render everything."""
    return os.environ.get("FORCE_RERUN", "").lower() in ("1", "true", "yes")


def r2_exists(r2, key):
    try:
        r2.head_object(Bucket=fu.R2_BUCKET, Key=key)
        return True
    except Exception:
        return False


# ── UKV side ──────────────────────────────────────────────────────────────────
def build_station_pixels():
    """Map gauge stations onto the UKV output grid (same maths as fetch_ukv.main)."""
    if not os.path.exists("rain/stations.json"):
        return {}
    from pyproj import Transformer
    with open("rain/stations.json") as f:
        stations = json.load(f).get("stations", {})
    tf = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    mx0, my0 = tf.transform(fu.LON_MIN, fu.LAT_MIN)
    mx1, my1 = tf.transform(fu.LON_MAX, fu.LAT_MAX)
    pixels = {}
    for sid, s in stations.items():
        mx, my = tf.transform(s["lon"], s["lat"])
        col = round((mx - mx0) / (mx1 - mx0) * (fu.WIDTH - 1))
        row = round((my1 - my) / (my1 - my0) * (fu.HEIGHT - 1))
        if 0 <= row < fu.HEIGHT and 0 <= col < fu.WIDTH:
            pixels[sid] = (row, col)
    return pixels


def build_mapping_for(s3, run_ts, steps):
    """Build the LAEA→Mercator mapping from a run's first step file."""
    _, first_offset, first_valid = steps[0]
    nc_bytes = fu.download_nc(s3, run_ts, first_valid, first_offset, "rainfall_rate")
    if nc_bytes is None:
        return None
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp.write(nc_bytes)
        path = tmp.name
    try:
        return fu.build_mapping(path)
    finally:
        os.unlink(path)


def process_ukv_run(s3, r2, event_id, run_ts, mapping, masks, station_pixels):
    """Re-render one UKV run into the event prefix. Returns its manifest entry.

    Mirrors fetch_ukv.main()'s per-step loop — the same accumulation stack, the
    same estimated-slot handling, the same poly/gauge/area-timeseries outputs —
    differing only in the R2 keys written. Kept as its own copy rather than
    refactored out of main(): main() is the live pipeline running every three
    hours, and reshaping it around a one-off replay is regression risk for no
    operational gain.
    """
    ukv_base       = event_key(event_id, "ukv")
    poly_prefix    = event_key(event_id, "ukv_poly")
    gauge_prefix   = event_key(event_id, "ukv_gauge")
    area_ts_key    = event_key(event_id, "ukv_area_ts", f"{run_ts}.json")

    steps = fu.discover_steps(s3, run_ts)
    if not steps:
        print(f"  {run_ts}: no whole-hour steps found — skipping")
        return None
    print(f"  {run_ts}: {len(steps)} steps, T+{steps[0][0]}h → T+{steps[-1][0]}h")

    step_entries   = []
    area_ts_steps  = []
    accum_stack    = []

    for i, (hours, offset, valid_ts) in enumerate(steps):
        vlabel = event_valid_label(valid_ts)
        print(f"    [{i+1}/{len(steps)}] {offset}  {vlabel}", flush=True)

        entry = {"offset": offset, "offset_hours": hours, "valid_label": vlabel}

        arr, arr_1h = fu.load_arr_both(s3, run_ts, valid_ts, offset, mapping)

        urls = fu.upload_schemes(arr, fu.RATE_SCHEMES, run_ts, offset, "rate", base=ukv_base)
        if urls:
            entry["rainfall_rate"] = urls

        if arr_1h is not None:
            accum_stack.append({"arr": arr_1h, "hours": 1, "estimated": False})
        elif arr is not None:
            step_h = hours - steps[i - 1][0] if i > 0 else hours
            print(f"        [estimated] rate×{step_h}h as accum proxy")
            accum_stack.append({"arr": arr * step_h, "hours": step_h, "estimated": True})

        total_stk = sum(s["hours"] for s in accum_stack)
        while total_stk > 48 and accum_stack:
            total_stk -= accum_stack.pop(0)["hours"]

        if not accum_stack:
            step_entries.append(entry)
            continue

        total_stk = sum(s["hours"] for s in accum_stack)
        arrays_for_poly = {}
        any_estimated   = False

        accum_renders = []
        for n in fu.ACCUM_PERIODS:
            if total_stk < n:
                continue
            covered, arrs, is_est = 0, [], False
            for slot in reversed(accum_stack):
                if covered >= n:
                    break
                if slot["hours"] <= n - covered:
                    arrs.append(slot["arr"])
                    covered += slot["hours"]
                    if slot["estimated"]:
                        is_est = True
                else:
                    break
            if covered < n:
                continue
            accum_renders.append((n, np.sum(arrs, axis=0).astype(np.float32), is_est))

        def _upload_accum(n, arr_n, is_est):
            urls = fu.upload_schemes(arr_n, fu.ACCUM_SCHEMES, run_ts, offset,
                                     f"accum_{n}h", base=ukv_base)
            splats = {}
            dur_key = f"{n}h"
            if dur_key in fu.SPLAT_THRESHOLDS:
                for th in fu.SPLAT_THRESHOLDS[dur_key]:
                    th_str = f"{int(th)}mm" if th == int(th) else f"{th}mm"
                    splat_img = fu.render_splat_png(arr_n, th)
                    skey = f"{ukv_base}/{run_ts}/{offset}_splat_{dur_key}_{th_str}.png"
                    fu.png_to_r2(fu._r2_tl(), skey, splat_img)
                    splats[f"{dur_key}_{th_str}"] = f"{fu.R2_PUBLIC_URL}/{skey}"
            return n, arr_n, urls, is_est, splats

        if accum_renders:
            with ThreadPoolExecutor(max_workers=min(len(accum_renders), 6)) as ex:
                futs = [ex.submit(_upload_accum, n, a, e) for n, a, e in accum_renders]
                for fut in as_completed(futs):
                    n, arr_n, urls, is_est, splats = fut.result()
                    entry[f"accum_{n}h"] = urls
                    arrays_for_poly[f"accum_{n}h"] = arr_n
                    if splats:
                        entry.setdefault("splats", {}).update(splats)
                    if is_est:
                        any_estimated = True

        if any_estimated:
            entry["accum_estimated"] = True

        if arrays_for_poly and masks:
            poly_data = fu.compute_poly_averages(arrays_for_poly, masks)

            def _upload_poly():
                fu._r2_tl().put_object(
                    Bucket=fu.R2_BUCKET,
                    Key=f"{poly_prefix}/{run_ts}/{offset}.json",
                    Body=json.dumps(poly_data).encode(),
                    ContentType="application/json; charset=utf-8",
                )

            gauge_body = None
            if station_pixels:
                gauge_data = {}
                for sid, (row, col) in station_pixels.items():
                    vals = {k: round(float(a[row, col]), 2)
                            for k, a in arrays_for_poly.items()
                            if float(a[row, col]) >= 0}
                    if vals:
                        gauge_data[sid] = vals
                if gauge_data:
                    gauge_body = json.dumps(gauge_data, separators=(",", ":")).encode()

            def _upload_gauge():
                if gauge_body:
                    fu._r2_tl().put_object(
                        Bucket=fu.R2_BUCKET,
                        Key=f"{gauge_prefix}/{run_ts}/{offset}.json",
                        Body=gauge_body,
                        ContentType="application/json; charset=utf-8",
                    )

            with ThreadPoolExecutor(max_workers=2) as ex:
                list(ex.map(lambda fn: fn(), [_upload_poly, _upload_gauge]))

            ts_entry = {"hours": hours}
            for layer_name in masks:
                ts_entry[layer_name] = poly_data.get(layer_name, {}).get("accum_1h", {})
            area_ts_steps.append(ts_entry)

        step_entries.append(entry)

    if area_ts_steps:
        area_ts = {"step_hours": [s["hours"] for s in area_ts_steps]}
        for layer_name in masks:
            area_ts[layer_name] = {}
            for area_name in {a for s in area_ts_steps for a in s.get(layer_name, {})}:
                area_ts[layer_name][area_name] = [
                    s.get(layer_name, {}).get(area_name) for s in area_ts_steps
                ]
        r2.put_object(
            Bucket=fu.R2_BUCKET, Key=area_ts_key,
            Body=json.dumps(area_ts, separators=(",", ":")).encode(),
            ContentType="application/json; charset=utf-8",
        )

    return {
        "run_ts":         run_ts,
        "run_label":      event_run_label(run_ts),
        "forecast_hours": steps[-1][0],
        "steps":          step_entries,
    }


def select_runs(event_id, runs, only_runs, shard, shards):
    """Narrow an event's run list by explicit selection or by shard.

    Sharding is round-robin rather than contiguous so that the 03Z/15Z runs —
    the ones that publish a 3-hourly tail past T+54h and so carry ~40% more
    steps — spread evenly across jobs instead of piling into one long shard.
    """
    if only_runs:
        wanted = {r.strip() for r in only_runs.split(",") if r.strip()}
        unknown = wanted - set(runs)
        if unknown:
            print(f"Runs not in event {event_id}: {sorted(unknown)}")
            sys.exit(1)
        return [r for r in runs if r in wanted]
    if shards > 1:
        if not 1 <= shard <= shards:
            print(f"--shard must be between 1 and --shards ({shards})")
            sys.exit(1)
        return [r for i, r in enumerate(runs) if i % shards == shard - 1]
    return runs


def mode_ukv(r2, event_id, event, only_runs, shard, shards):
    s3 = fu.get_s3()
    runs = select_runs(event_id, event_runs(event), only_runs, shard, shards)
    if not runs:
        print("No runs selected — nothing to do.")
        return
    if shards > 1 and not only_runs:
        print(f"Shard {shard}/{shards}")
    print(f"Event {event_id}: {len(runs)} run(s) to process")

    print("Loading polygon masks...")
    # Reuses fetch_ukv's own ukv_masks/ cache deliberately. CLAUDE.md warns
    # against *sharing* a mask prefix between pipelines, but that hazard is
    # about two writers disagreeing on the cached geojson fingerprint. This
    # calls fetch_ukv's own loader with fetch_ukv's own layers on fetch_ukv's
    # own grid, so the fingerprint it computes is the one already cached — a
    # read, not a competing write.
    masks = fu.load_or_build_ukv_masks(r2)
    print(f"  Masks ready for {len(masks)} boundary layer(s)")

    station_pixels = build_station_pixels()
    print(f"  {len(station_pixels)} gauge stations mapped to UKV grid")

    mapping  = None
    ok, fail = [], []
    for run_ts in runs:
        frag_key = event_key(event_id, "runs", f"{run_ts}.json")
        if r2_exists(r2, frag_key) and not _force_rerun():
            print(f"  {run_ts}: already processed — skipping (FORCE_RERUN=1 to redo)")
            ok.append(run_ts)
            continue

        if not fu.is_run_complete(s3, run_ts):
            print(f"  {run_ts}: incomplete on Met Office S3 (no T+54h) — skipping")
            fail.append(run_ts)
            continue

        steps = fu.discover_steps(s3, run_ts)
        if not steps:
            fail.append(run_ts)
            continue

        if mapping is None:
            # The UKV grid is static across runs, so this is built once and
            # reused for every run in this shard.
            print("  Building coordinate mapping...")
            mapping = build_mapping_for(s3, run_ts, steps)
            if mapping is None:
                print("  Cannot build mapping — aborting.")
                sys.exit(1)

        entry = process_ukv_run(s3, r2, event_id, run_ts, mapping, masks, station_pixels)
        if entry is None:
            fail.append(run_ts)
            continue

        fu.json_to_r2(r2, frag_key, entry)
        print(f"  {run_ts}: done — {len(entry['steps'])} steps")
        ok.append(run_ts)

    print(f"\nUKV backfill complete: {len(ok)} ok, {len(fail)} failed/skipped")
    if fail:
        print(f"  Not processed: {', '.join(fail)}")


# ── Radar side ────────────────────────────────────────────────────────────────
def mode_radar(r2, event_id, event):
    """Copy the observed side into the event prefix before its 14-day window shuts.

    Server-side copy_object rather than download+upload: same Class A cost as a
    PUT, but no egress and no bytes through this process.
    """
    all_ts = radar_snapshot_ts(event)
    print(f"Event {event_id}: {len(all_ts)} radar snapshot slots "
          f"({event['radar_start']} → {event['radar_end']})")

    def copy_one(src_key, dst_key):
        if r2_exists(r2, dst_key):
            return "skip"
        if not r2_exists(r2, src_key):
            return "missing"
        r2.copy_object(Bucket=fu.R2_BUCKET, Key=dst_key,
                       CopySource={"Bucket": fu.R2_BUCKET, "Key": src_key})
        return "copied"

    present, counts = [], {"copied": 0, "skip": 0, "missing": 0}

    for n, ts in enumerate(all_ts, 1):
        jobs = []
        for period in RADAR_PERIODS:
            for scheme in RADAR_SCHEMES:
                name = f"{ts}_{period}_{scheme}.png"
                jobs.append((f"accum_hist/{name}", event_key(event_id, "accum_hist", name)))
        jobs.append((f"accum_poly/{ts}.json", event_key(event_id, "accum_poly", f"{ts}.json")))
        jobs.append((f"gauge_bias/{ts}.json", event_key(event_id, "gauge_bias", f"{ts}.json")))

        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(lambda j: copy_one(*j), jobs))
        for r in results:
            counts[r] += 1

        # A snapshot counts as present only if its imagery came across; a
        # timestamp whose PNGs have already expired must not reach the manifest
        # or the page would show a broken frame rather than skipping it.
        if any(r != "missing" for r in results[:-2]):
            present.append(ts)

        if n % 24 == 0 or n == len(all_ts):
            print(f"  [{n}/{len(all_ts)}] {ts}  "
                  f"copied={counts['copied']} skipped={counts['skip']} missing={counts['missing']}",
                  flush=True)

    fu.json_to_r2(r2, event_key(event_id, "radar_snapshots.json"),
                  {"snapshots": present})
    print(f"\nRadar backfill complete: {len(present)}/{len(all_ts)} snapshots present")
    print(f"  copied={counts['copied']} already-present={counts['skip']} missing={counts['missing']}")
    if len(present) < len(all_ts):
        print("  Missing slots are timestamps whose live objects had already "
              "expired or were never written (radar outage).")


# ── Expiry / purge ────────────────────────────────────────────────────────────
def event_expired(event, today=None):
    """True once the event's `expires` date has arrived. No date => never expires."""
    exp = event.get("expires")
    if not exp:
        return False
    today = today or datetime.utcnow().date()
    return today >= datetime.strptime(exp, "%Y-%m-%d").date()


def _remove_menu_card(menu_path, href):
    """Strip the event's card from menu.html. Returns True if anything changed."""
    if not os.path.exists(menu_path):
        return False
    src = open(menu_path).read()
    start_tag = f'<a href="{href}" class="card">'
    i = src.find(start_tag)
    if i < 0:
        return False
    end = src.find("</a>", i)
    if end < 0:
        return False
    end += len("</a>")
    # Take the card's leading indentation and its trailing newline with it, so
    # removing a card leaves no blank gap behind in the grid.
    line_start = src.rfind("\n", 0, i) + 1
    while end < len(src) and src[end] in "\r\n":
        end += 1
    open(menu_path, "w").write(src[:line_start] + src[end:])
    return True


def mode_purge(r2, event_id, event, force=False):
    """Delete an expired event: its whole R2 prefix, its page, its menu card.

    Deliberately refuses to run before the expiry date unless --force is given,
    so a mis-scheduled workflow cannot bin a live case study early. Deletes are
    free on R2 (DeleteObject is not a Class A operation); only the LIST pages
    cost anything.
    """
    if not event_expired(event) and not force:
        print(f"Not expired (expires {event.get('expires', 'never')}) — nothing to do.")
        return

    prefix = event_key(event_id) + "/"
    # Guard: this function only ever deletes inside the event tree. Anything
    # else means a malformed event id, and we stop rather than issue deletes.
    if not prefix.startswith(EVENT_ROOT + "/") or prefix.count("/") < 2:
        print(f"Refusing to purge unsafe prefix {prefix!r}")
        sys.exit(1)

    print(f"Purging R2 prefix {prefix}")
    deleted = 0
    paginator = r2.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=fu.R2_BUCKET, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            r2.delete_objects(Bucket=fu.R2_BUCKET, Delete={"Objects": batch})
            deleted += len(batch)
            print(f"  deleted {deleted} objects...", flush=True)
    print(f"R2 purge complete: {deleted} object(s) removed under {prefix}")

    page_dir = event.get("page_dir")
    if page_dir and os.path.isdir(page_dir):
        import shutil
        shutil.rmtree(page_dir)
        print(f"Removed page directory {page_dir}/")

    href = event.get("menu_href")
    if href and _remove_menu_card("menu.html", href):
        print(f"Removed menu card for {href}")

    print(f"\nEvent '{event_id}' purged. Commit the working tree to finish.")


# ── Meta assembly ─────────────────────────────────────────────────────────────
def frame_label(ts):
    return datetime.strptime(ts, "%Y%m%d%H%M").strftime("%a %d %b %Y %H:%M UTC")


def radar_snapshot_entry(event_id, ts):
    """Rebuild an accum_multi_meta-shaped snapshot entry pointing at the event prefix."""
    end_dt = datetime.strptime(ts, "%Y%m%d%H%M")
    entry = {"ts": ts, "time": frame_label(ts)}
    for period, n_frames in RADAR_PERIODS.items():
        start_dt = end_dt - timedelta(minutes=15 * n_frames)
        data = {
            "period_start": start_dt.strftime("%a %d %b %Y %H:%M UTC"),
            "period_end":   frame_label(ts),
        }
        for scheme in RADAR_SCHEMES:
            key = event_key(event_id, "accum_hist", f"{ts}_{period}_{scheme}.png")
            data[scheme] = f"{fu.R2_PUBLIC_URL}/{key}"
        entry[period] = data
    return entry


def normalise_labels(frag):
    """Re-derive a fragment's time labels from its own timestamps.

    Labels are *derived at assembly*, not trusted from the fragment, so the
    manifest has exactly one source of truth for how a time is written. A
    fragment rendered by an older revision of this script — or by one whose
    labels came from fetch_ukv's UK-local helpers — is normalised here rather
    than needing its imagery re-rendered to fix a string. That is the whole
    point: re-running `--mode meta` costs seconds, re-running `--mode ukv`
    costs ~30k Class A operations.
    """
    run_ts = frag.get("run_ts")
    if not run_ts:
        return frag
    frag["run_label"] = event_run_label(run_ts)
    run_dt = fu.parse_run_dt(run_ts)
    for step in frag.get("steps", []):
        hours = step.get("offset_hours")
        if hours is None:
            continue
        valid_ts = (run_dt + timedelta(hours=hours)).strftime("%Y%m%dT%H%MZ")
        step["valid_label"] = event_valid_label(valid_ts)
    return frag


def mode_meta(r2, event_id, event):
    runs_wanted = event_runs(event)

    run_entries = []
    for run_ts in runs_wanted:
        frag = fu.json_from_r2(r2, event_key(event_id, "runs", f"{run_ts}.json"))
        if frag:
            run_entries.append(normalise_labels(frag))
        else:
            print(f"  {run_ts}: no fragment on R2 — omitted from manifest")

    if not run_entries:
        print("No run fragments found — run --mode ukv first.")
        sys.exit(1)

    # Newest run first, matching ukv_meta.json's ordering so the page's run
    # selector reads the same way as the live one.
    run_entries.sort(key=lambda r: r["run_ts"], reverse=True)

    radar_idx = fu.json_from_r2(r2, event_key(event_id, "radar_snapshots.json"), default={})
    snap_ts   = radar_idx.get("snapshots", [])
    if not snap_ts:
        print("  No radar snapshots indexed — run --mode radar first.")
    snapshots = [radar_snapshot_entry(event_id, ts) for ts in sorted(snap_ts)]

    meta = {
        "event_id":     event_id,
        "title":        event["title"],
        "subtitle":     event["subtitle"],
        "description":  event["description"],
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00.000Z"),
        "base":         event_key(event_id),
        "event_start":  event.get("event_start"),
        "event_end":    event.get("event_end"),
        "focus_ts":     event.get("focus_ts"),
        "default_run":  event.get("default_run"),
        "runs":         run_entries,
        "snapshots":    snapshots,
    }
    fu.json_to_r2(r2, event_key(event_id, "meta.json"), meta)
    print(f"\nManifest written: {len(run_entries)} run(s), {len(snapshots)} radar snapshot(s)")
    print(f"  {fu.R2_PUBLIC_URL}/{event_key(event_id, 'meta.json')}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", required=True, choices=("ukv", "radar", "meta", "purge"))
    ap.add_argument("--event", default="early_sep_2026")
    ap.add_argument("--runs", default="",
                    help="Comma-separated run timestamps to process (ukv mode only). "
                         "Omit to process every run in the event.")
    ap.add_argument("--shard", type=int, default=1,
                    help="1-based shard index for this job (ukv mode only).")
    ap.add_argument("--shards", type=int, default=1,
                    help="Total number of shards the event's runs are split across.")
    ap.add_argument("--force", action="store_true",
                    help="purge mode only: bin the event even if its expiry date "
                         "has not arrived yet.")
    args = ap.parse_args()

    if args.event not in EVENTS:
        print(f"Unknown event '{args.event}'. Known: {', '.join(EVENTS)}")
        sys.exit(1)
    event = EVENTS[args.event]

    if not fu.USE_R2:
        print("R2 credentials not configured — set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, "
              "R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_PUBLIC_BASE_URL.")
        sys.exit(1)

    r2 = fu.get_r2()
    print(f"=== {event['title']} — mode: {args.mode} ===")

    if args.mode == "ukv":
        mode_ukv(r2, args.event, event, args.runs, args.shard, args.shards)
    elif args.mode == "radar":
        mode_radar(r2, args.event, event)
    elif args.mode == "purge":
        mode_purge(r2, args.event, event, args.force)
    else:
        mode_meta(r2, args.event, event)


if __name__ == "__main__":
    main()
