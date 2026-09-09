#!/usr/bin/env python3
"""
make_catalog.py

Generates catalog.json — a bucket-wide index of every R2 prefix/root object
this project writes, with its purpose, key pattern, timestamp format, "latest"
pointer (if any), and retention policy. This is the one thing nothing else in
the bucket provides: a starting point for a consumer (an LLM forecasting
agent, a new script, a future maintainer) to discover what's in R2 without
reading Python source.

This is documentation-as-data, not live telemetry — the prefix list below is
hand-maintained. When a script starts writing under a new prefix, or a
retention window changes, update PREFIXES/ROOT_OBJECTS here too.
"""

import hashlib
import json
import os
from datetime import datetime, timezone

R2_ACCOUNT_ID    = os.environ.get("R2_ACCOUNT_ID", "").strip()
R2_ACCESS_KEY    = os.environ.get("R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_KEY    = os.environ.get("R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET        = os.environ.get("R2_BUCKET_NAME", "").strip()
R2_PUBLIC_URL    = os.environ.get("R2_PUBLIC_BASE_URL", "").strip()
USE_R2 = all([R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET])

CATALOG_KEY = "catalog.json"
SCHEMA_VERSION = 1

# ── Prefixes (folders) ───────────────────────────────────────────────────────
# timestamp_format: strftime-style pattern embedded in keys under this prefix,
# or null if the prefix holds a small fixed set of non-timestamped objects.
# latest_pointer: key (relative to bucket root) that always points at the
# current/most-recent object for this prefix, or null if a consumer must
# LIST the prefix to find the newest key.
PREFIXES = [
    {
        "prefix": "uk-deterministic-2km/",
        "description": "Raw Met Office UKV NetCDF mirror (internal working data, not intended for direct consumption)",
        "written_by": "fetch_ukv.py",
        "key_pattern": "uk-deterministic-2km/{run_ts}/{valid_ts}-{offset}-{param}.nc",
        "timestamp_format": "YYYYMMDDTHHMMZ",
        "latest_pointer": None,
        "retention": "pruned with the UKV run cycle (superseded runs deleted)",
    },
    {
        "prefix": "ukv/",
        "description": "Rendered UKV forecast PNGs per run/offset/scheme (rainfall rate, thresholds)",
        "written_by": "fetch_ukv.py",
        "key_pattern": "ukv/{run_ts}/{offset}_{prefix}_{scheme}.png",
        "timestamp_format": "YYYYMMDDTHHMMZ",
        "latest_pointer": "ukv_meta.json",
        "retention": "pruned with the UKV run cycle",
    },
    {
        "prefix": "ukv_poly/",
        "description": "UKV forecast rainfall polygons per run/offset (vector, for map overlay)",
        "written_by": "fetch_ukv.py",
        "key_pattern": "ukv_poly/{run_ts}/{offset}.json",
        "timestamp_format": "YYYYMMDDTHHMMZ",
        "latest_pointer": None,
        "retention": "pruned with the UKV run cycle",
        "notes": "No direct latest pointer; discover the active run_ts via ukv_meta.json first.",
    },
    {
        "prefix": "ukv_gauge/",
        "description": "UKV forecast-vs-gauge comparison JSON per run/offset",
        "written_by": "fetch_ukv.py",
        "key_pattern": "ukv_gauge/{run_ts}/{offset}.json",
        "timestamp_format": "YYYYMMDDTHHMMZ",
        "latest_pointer": None,
        "retention": "pruned with the UKV run cycle",
    },
    {
        "prefix": "ukv_area_ts/",
        "description": "UKV forecast area time-series (rainfall trend per admin area) per run",
        "written_by": "fetch_ukv.py",
        "key_pattern": "ukv_area_ts/{run_ts}.json",
        "timestamp_format": "YYYYMMDDTHHMMZ",
        "latest_pointer": None,
        "retention": "pruned with the UKV run cycle",
    },
    {
        "prefix": "compare_events/",
        "description": "Frozen archive of a past rainfall event — regenerated UKV runs plus the radar/gauge observations they are verified against, for the pinned /compare-<event> pages",
        "written_by": "backfill_compare_event.py",
        "key_pattern": "compare_events/{event_id}/meta.json, compare_events/{event_id}/{ukv,ukv_poly,ukv_gauge,ukv_area_ts,accum_hist,accum_poly,gauge_bias}/...",
        "timestamp_format": "YYYYMMDDTHHMMZ (UKV runs), YYYYMMDDHHMM (radar snapshots)",
        "latest_pointer": "compare_events/{event_id}/meta.json",
        "retention": "permanent — deliberately outside every cleanup job",
        "notes": (
            "Mirrors the live ukv*/ and accum*/ key layouts one level down so the "
            "event pages can reuse /compare's loading code. Exists because those live "
            "prefixes are pruned at 72h and 14 days respectively, which would otherwise "
            "make any event unreviewable within a week. Nothing writes here on a "
            "schedule; it is populated by the Backfill Compare Event workflow."
        ),
    },
    {
        "prefix": "ukv_masks/",
        "description": "Static per-layer grid-cell mask arrays used to aggregate UKV cells into admin areas",
        "written_by": "fetch_ukv.py",
        "key_pattern": "ukv_masks/{layer_name}.npz, ukv_masks/{layer_name}_names.json, ukv_masks/{layer_name}_fp.json",
        "timestamp_format": None,
        "latest_pointer": None,
        "retention": "static — rebuilt only when boundary layers change",
    },
    {
        "prefix": "rain/readings/",
        "description": "15-minute rain gauge accumulation readings, one object per day",
        "written_by": "fetch_rain.py",
        "key_pattern": "rain/readings/{YYYYMMDD}.json",
        "timestamp_format": "YYYYMMDD",
        "latest_pointer": "rain/meta.json",
        "retention": "14 days",
    },
    {
        "prefix": "mslp/",
        "description": "GFS mean-sea-level-pressure chart overlays (SVG)",
        "written_by": "fetch_mslp.py",
        "key_pattern": "mslp/{YYYYMMDDHHMM}.svg",
        "timestamp_format": "YYYYMMDDHHMM",
        "latest_pointer": "mslp_meta.json",
        "retention": "short rolling window, see mslp_meta.json for current frames",
    },
    {
        "prefix": "warnings/archive/",
        "description": "Historical daily snapshots of Met Office weather warnings",
        "written_by": "fetch_warnings.py",
        "key_pattern": "warnings/archive/warnings_{YYYYMMDD}.json",
        "timestamp_format": "YYYYMMDD",
        "latest_pointer": "warnings/warnings_latest.json",
        "retention": "indefinite (append-only archive)",
    },
    {
        "prefix": "ea/floodareas/",
        "description": "Environment Agency flood area static reference (index, centroids, per-area polygons)",
        "written_by": "fetch_ea_floodareas.py",
        "key_pattern": "ea/floodareas/{index,centroids,polygons_all.min}.json, ea/floodareas/polygons/{code}.json",
        "timestamp_format": None,
        "latest_pointer": "ea/floodareas/index.json",
        "retention": "static — rebuilt when EA area definitions change",
    },
    {
        "prefix": "ea/",
        "description": "Environment Agency live flood warnings snapshot, timeline and severity-count series",
        "written_by": "fetch_ea_warnings.py",
        "key_pattern": "ea/floods/current.json, ea/timeline/events.json, ea/counts/series.json",
        "timestamp_format": "ISO-8601",
        "latest_pointer": "ea/floods/current.json",
        "retention": "current.json overwritten each run; timeline/counts are indefinite (append-only)",
    },
    {
        "prefix": "nrw/floodareas/",
        "description": "Natural Resources Wales flood area reference and live warnings (mirrors ea/ shape for Wales)",
        "written_by": "build_nrw_floodareas.py, fetch_nrw_warnings.py",
        "key_pattern": "nrw/floodareas/{index,centroids,polygons_all.min}.json, nrw/floodareas/polygons/{code}.json",
        "timestamp_format": None,
        "latest_pointer": "nrw/floodareas/index.json",
        "retention": "static reference; live snapshot overwritten each run",
    },
    {
        "prefix": "rfg/archive/",
        "description": "Archived Rapid Flood Guidance PDF bulletins",
        "written_by": "fetch_rfg.py",
        "key_pattern": "rfg/archive/rfg_{rfg_id}.pdf",
        "timestamp_format": None,
        "latest_pointer": "rfg/rfg_latest.json",
        "retention": "indefinite (append-only archive)",
    },
    {
        "prefix": "lightning_wis2/history/",
        "description": "Met Office ATDnet lightning strike history, one object per day",
        "written_by": "fetch_lightning_wis2.py",
        "key_pattern": "lightning_wis2/history/{YYYY-MM-DD}.json",
        "timestamp_format": "YYYY-MM-DD",
        "latest_pointer": "lightning_wis2/lightning_latest.json",
        "retention": "365 days",
    },
    {
        "prefix": "sat_gh/, radar_parallel/",
        "description": "Rendered satellite and radar frame images (PNG/JPG) used by the live viewer",
        "written_by": "process_latest.py, process_parallel.py",
        "key_pattern": "derived from the source .h5/satellite filename",
        "timestamp_format": "embedded in source filename",
        "latest_pointer": "frames_parallel.json, status.json",
        "retention": "3 days",
    },
    {
        "prefix": "composites/",
        "description": "Dashboard basemap+radar+satellite composite images and rainfall accumulation composites (WebP)",
        "written_by": "make_dashboard_composites.py, make_accum_hourly.py",
        "key_pattern": "composites/dashboard_{...}.webp, composites/accum_hourly_{YYYYMMDDHHMM}.webp, composites/accum_daily_{YYYYMMDD}.webp",
        "timestamp_format": "YYYYMMDDHHMM / YYYYMMDD",
        "latest_pointer": "frames_mslp.json, frames_accum_hourly.json, frames_accum_daily.json",
        "retention": "accum_hourly 48 hours, accum_daily 7 days",
    },
    {
        "prefix": "accum_poly/, accum_hist/, accum_masks/",
        "description": "Multi-duration (1h/3h/6h/24h/...) rainfall accumulation polygons and history",
        "written_by": "make_accum_multi.py",
        "key_pattern": "accum_poly/{YYYYMMDDHHMM}.json, accum_hist/...",
        "timestamp_format": "YYYYMMDDHHMM",
        "latest_pointer": "accum_multi_meta.json",
        "retention": "14 days",
    },
    {
        "prefix": "gaugecheck/",
        "description": "Rain gauge QC results, rolling flagging-event log, and per-run archive",
        "written_by": "gauge_qc.py",
        "key_pattern": "gaugecheck/results.json, gaugecheck/log.json, gaugecheck/manifest.json, gaugecheck/runs/{ts}.json",
        "timestamp_format": "ISO-8601",
        "latest_pointer": "gaugecheck/results.json (current), gaugecheck/manifest.json (run archive index)",
        "retention": "log.json 90 days; results.json/manifest.json current snapshot",
    },
    {
        "prefix": "gauge_bias/",
        "description": "Rain gauge bias-correction factors derived from radar/gauge comparison",
        "written_by": "make_gauge_bias.py",
        "key_pattern": "gauge_bias/...",
        "timestamp_format": None,
        "latest_pointer": "gauge_bias_meta.json",
        "retention": "current snapshot, overwritten each run",
    },
    {
        "prefix": "surface_obs/",
        "description": "Met Office surface weather station observations (temperature, wind, pressure, etc.)",
        "written_by": "surface_obs fetch script",
        "key_pattern": "surface_obs/latest_obs.geojson, surface_obs/latest_obs.min.json, surface_obs/history/{hour_tag}.geojson",
        "timestamp_format": "hour_tag",
        "latest_pointer": "surface_obs/latest_obs.geojson",
        "retention": "history retained on a rolling window; latest overwritten each run",
    },
    {
        "prefix": "geo/",
        "description": "Static boundary GeoJSONs (UK counties, regions, catchments, grid) used for map overlays and region classification",
        "written_by": "upload_boundary_geojsons.py",
        "key_pattern": "geo/{filename}.geojson",
        "timestamp_format": None,
        "latest_pointer": None,
        "retention": "static — reuploaded only when the source geojson changes",
    },
    {
        "prefix": "flood_history/",
        "description": "Aggregated historical flood statistics (per-area and overall) and event archive",
        "written_by": "upload_flood_history.py, build_flood_stats.py",
        "key_pattern": "flood_history/...",
        "timestamp_format": None,
        "latest_pointer": None,
        "retention": "static/periodically rebuilt, no automatic pruning",
    },
    {
        "prefix": "cosmos/",
        "description": "COSMOS-UK soil moisture network — daily volumetric water content readings by station",
        "written_by": "make_soil.py",
        "key_pattern": "cosmos/stations.json, cosmos/{YYYYMMDD}.json, cosmos/meta.json",
        "timestamp_format": "YYYYMMDD",
        "latest_pointer": "cosmos/meta.json",
        "retention": "rolling ~14-day window (re-fetched and overwritten each run)",
    },
    {
        "prefix": "status/",
        "description": "GitHub Actions workflow run health — rolling per-job success/failure history",
        "written_by": "report_job_status.py (called at the end of most workflows)",
        "key_pattern": "status/history.json",
        "timestamp_format": "ISO-8601",
        "latest_pointer": "status/history.json",
        "retention": "last 30 runs per job",
    },
]

# ── Root-level objects ───────────────────────────────────────────────────────
ROOT_OBJECTS = [
    {"key": "ukv_meta.json", "description": "UKV run manifest — active run_ts, available offsets/schemes", "written_by": "fetch_ukv.py", "dual_homed_in_git": True},
    {"key": "ukv_trends.json", "description": "UKV multi-run trend index", "written_by": "fetch_ukv.py", "dual_homed_in_git": True},
    {"key": "mslp_meta.json", "description": "MSLP chart frame manifest", "written_by": "fetch_mslp.py", "dual_homed_in_git": True},
    {"key": "accum_multi_meta.json", "description": "Multi-duration rainfall accumulation manifest", "written_by": "make_accum_multi.py", "dual_homed_in_git": True},
    {"key": "accum_24hr_meta.json", "description": "24-hour rainfall accumulation manifest", "written_by": "make_accum_24hr.py", "dual_homed_in_git": True},
    {"key": "gauge_bias_meta.json", "description": "Gauge bias-correction manifest", "written_by": "make_gauge_bias.py", "dual_homed_in_git": True},
    {"key": "frames.json", "description": "Legacy radar frame manifest", "written_by": "process_latest.py", "dual_homed_in_git": True},
    {"key": "frames_5m.json", "description": "OPERA 5-minute radar frame manifest", "written_by": "fetch_opera.py", "dual_homed_in_git": True},
    {"key": "frames_parallel.json", "description": "Current radar/satellite frame manifest used by the live viewer", "written_by": "process_parallel.py", "dual_homed_in_git": True},
    {"key": "frames_mslp.json", "description": "MSLP composite frame manifest", "written_by": "make_dashboard_composites.py", "dual_homed_in_git": True},
    {"key": "frames_accum_hourly.json", "description": "Hourly accumulation composite frame manifest", "written_by": "make_accum_hourly.py", "dual_homed_in_git": True},
    {"key": "frames_accum_daily.json", "description": "Daily accumulation composite frame manifest", "written_by": "make_accum_hourly.py", "dual_homed_in_git": True},
    {"key": "status.json", "description": "Radar frame count / last-updated pointer", "written_by": "process_latest.py", "dual_homed_in_git": True},
    {"key": "catalog.json", "description": "This file — bucket-wide structural index", "written_by": "make_catalog.py", "dual_homed_in_git": False},
]

EXTERNAL_REFERENCES = [
    {
        "note": "agent/summary.json (an AI-generated weather briefing assembled from most of the sources catalogued here) is committed to the git repo and served via GitHub Pages — it is NOT mirrored to R2.",
    },
]


def r2_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


def r2_put_json(r2, key, obj):
    body = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    # Skip the Class A PUT when unchanged — R2's ETag for a single-part
    # put_object is the body MD5, and head_object is Class B (cheap).
    try:
        head = r2.head_object(Bucket=R2_BUCKET, Key=key)
        if head.get("ETag", "").strip('"') == hashlib.md5(body).hexdigest():
            print(f"  {key} unchanged — skipping PUT")
            return
    except Exception:
        pass
    r2.put_object(
        Bucket=R2_BUCKET, Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        CacheControl="no-cache, max-age=0",
    )
    print(f"  Uploaded to R2: {key} ({len(body):,} bytes)")


def build_catalog():
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bucket_public_base_url": R2_PUBLIC_URL or "https://radar.floodforecast.co.uk",
        "description": (
            "Index of every prefix and root object this project writes to R2. "
            "Hand-maintained in make_catalog.py — update it there when a script "
            "starts writing under a new prefix or a retention policy changes."
        ),
        "prefixes": PREFIXES,
        "root_objects": ROOT_OBJECTS,
        "external_references": EXTERNAL_REFERENCES,
    }


def main():
    catalog = build_catalog()

    os.makedirs(os.path.dirname(CATALOG_KEY) or ".", exist_ok=True)
    with open(CATALOG_KEY, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
    print(f"Wrote local {CATALOG_KEY} ({len(PREFIXES)} prefixes, {len(ROOT_OBJECTS)} root objects)")

    if not USE_R2:
        print("R2 credentials not set — skipping R2 upload")
        return

    r2 = r2_client()
    r2_put_json(r2, CATALOG_KEY, catalog)


if __name__ == "__main__":
    main()
