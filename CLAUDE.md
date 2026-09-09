# Radar / Flood Forecast — Claude notes

## Workflow

- **Always push committed changes to `main` on GitHub.** The user wants commits pushed, not left local — after committing, push to `origin main` (rebasing on top of the frequent automated data-update commits if the remote has moved on) without needing to be asked each time.

## UI conventions

- **No block caps / all-caps in the UI.** The user dislikes `text-transform: uppercase` and writing labels in ALL CAPS. Use sentence case or title case instead. This applies to table headers, legend labels, status pills, modal text, and any UI copy.
- Labels and pills use title case for proper nouns and single-word abbreviations (e.g. "Spike", "Spatial", "Genuine"), not "SPIKE", "SPATIAL", "GENUINE".

## Project structure

- Main viewer: `index.html` (Leaflet radar + satellite + MSLP)
- Rain gauges: `rain/index.html`
- Gauge QC: `gaugecheck.html` + `gauge_qc.py`
- Data served from Cloudflare R2 via `https://radar.floodforecast.co.uk`

## CARTO basemap API key

Every map in this repo uses CARTO's raster basemaps, which since August 2026
stamp an "API key required" watermark across each tile unless the request
carries a key. Ours is `cb1_27ic_1_fde6c8c094980efec9791cfc` (free tier, 5M
tile requests/month, no domain/referer restriction — verified). **Any new
CARTO tile URL must append `?key=cb1_27ic_1_fde6c8c094980efec9791cfc`**, or
that page silently renders watermarked tiles.

- The key is inherently public (it ships in client-side HTML on every page),
  so it is committed in plain text rather than injected as a secret — but keep
  the CARTO + OpenStreetMap attribution visible, which is what the free tier
  is in exchange for.
- Both CARTO hosts in use honour the key: `{s}.basemaps.cartocdn.com`
  (all the Leaflet pages, `voyager` and `dark_all`, retina and non-retina)
  and the legacy `cartodb-basemaps-a.global.ssl.fastly.net` (`light_all`,
  used server-side by `make_dashboard_composites.py`).
- **`make_dashboard_composites.py` bakes basemap tiles into cached PNGs**
  (`static/dashboard_basemap_*.png`) that survive across runs via the GitHub
  Actions cache in `radar_parallel.yml`, so changing `TILE_URL` alone will
  *not* refresh them. Bump `BASEMAP_VERSION` whenever `TILE_URL` changes — it
  is part of the `.meta` cache-validity string, and without it a watermarked
  basemap would stay baked into the dashboard/MSLP composites indefinitely.
- Vector basemaps do not require a key yet; CARTO say they will notify before
  that changes, and the same key will cover it.

## R2 Class A budget

The Cloudflare R2 free tier allows **1M Class A operations/month**
(PutObject, CopyObject, ListObjects — LIST costs one op *per page*).
Class B (GetObject, HeadObject) has a 10M allowance and is effectively
free at our volumes; DeleteObject is completely free. The fetch scripts
run every 5–15 minutes, so per-run waste multiplies fast. Rules for any
script that writes to R2:

- **Never unconditionally PUT content that may be unchanged.** R2's ETag
  for a single-part upload is the body MD5, so `head_object` + MD5 compare
  (Class B) tells you whether a PUT (Class A) can be skipped. See
  `r2_upload()` in `make_accum_hourly.py` or `r2_upload_file()` in
  `make_dashboard_composites.py` for the pattern.
- **Don't re-LIST a prefix you already listed.** Retention cleanups should
  reuse the key listing fetched at startup (deletes are free), not paginate
  the prefix again — see `cleanup_r2()` in `process_latest.py`.
- **Gate LIST-based prune scans to ~hourly** (`utcnow().minute < 10`)
  rather than every invocation; expired objects only cross a retention
  cutoff once an hour at most.
- The dominant remaining consumer is `fetch_ukv.py` (~25–30 PNG/JSON
  uploads per forecast step × ~55 steps × 8 runs/day ≈ 350k ops/month) —
  those are genuinely new objects each run, so reducing them means
  reducing schemes/thresholds/durations (a product decision, not plumbing).
  `fetch_ecmwf.py` adds a smaller ~76k ops/month on top of that (one
  scheme × up to 5 uploads/step × ~504 steps/day, across its 4 daily runs'
  hourly-to-T+90h cadence and up-to-15-day horizon) — combined R2 Class A
  usage is still well under 45% of the 1M/month free tier.

## Freshness timestamps are a public API (floodforecast consumer contract)

The sibling **floodforecast** Flask app reads the timestamp fields out of the
manifests this repo publishes and uses them to tell users, on the page,
whether what they are looking at is current. That makes these fields a
consumer contract rather than an internal detail. Nothing here needs a UI
change in this repo; it needs the fields to keep behaving.

| Object | Field floodforecast grades |
|---|---|
| `frames_parallel.json` | `time` on the last element (`Wed 12 Aug 2026 16:30 UTC`) |
| `rain/meta.json` | `latest_time` |
| `warnings/warnings_latest.json` | `generated_at` |
| `ea/floods/current.json`, `nrw/floods/current.json` | `generated_at` |
| `accum_multi_meta.json` | `generated_at` |
| `mslp_meta.json` | `generated_at` |
| `ukv_meta.json` | `runs[0].run_ts` |
| `ecmwf_meta.json` | `runs[0].run_ts` |

Three things to keep in mind when changing any of them:

- **Renaming or dropping one of these fields silently breaks a user-facing
  warning.** It does not fail loudly — floodforecast just stops being able to
  tell users their data is stale, which is the exact failure the feature
  exists to prevent. Treat them as public API.
- **A cadence change needs a matching threshold change in floodforecast**,
  because thresholds are 3× cadence for "late" and 6× for "very late". The
  live example is the Met Office warnings move from 30 to 10 minutes: that
  takes "late" from 90 minutes down to 30. Changing the schedule without
  telling the consumer leaves it either crying wolf or asleep.
- **`mslp_meta.json`, `ukv_meta.json` and `ecmwf_meta.json` frames run into
  the future** — they are forecast series, so the newest frame's
  `valid_time` is *ahead* of now (MSLP was +324 minutes ahead of its own
  `generated_at` when this was written). A consumer must never treat
  newest-frame time as data age; it would read as permanently fresh no
  matter how long the pipeline had been dead. `mslp_meta.json` also carries
  **no run timestamp at all** — only `generated_at` and per-frame
  `valid_time` — so if run-age grading is ever wanted for MSLP, this repo
  has to publish a run field first.

## UKV forecast fetch (`fetch_ukv.py`)

- Met Office publishes UKV runs **hourly**, but only the 3-hourly synoptic
  runs (00/03/06/09/12/15/18/21Z) go out to the full T+54h forecast horizon.
  The runs in between (e.g. 19Z, 20Z) only ever publish out to **T+11h** —
  this is expected Met Office behaviour, not a stall or an outage.
- `is_run_complete()` checks for the run's T+54h file on Met Office's public
  S3 bucket (`met-office-atmospheric-model-data`, region `eu-west-2`, prefix
  `uk-deterministic-2km/`) before accepting a run as processable. Since only
  the 3-hourly runs ever reach T+54h, a run showing "not yet complete" for
  a long time is only actually stuck if it's one of the 00/03/06/09/12/15/18/21Z
  runs — if it's an off-cycle hourly run, it will never complete and that's
  correct, `find_latest_run` will move on once a genuine 3-hourly run appears.
  Browse the bucket directly to check current state, e.g.:
  `https://s3.console.aws.amazon.com/s3/buckets/met-office-atmospheric-model-data?region=eu-west-2&prefix=uk-deterministic-2km/`
  (needs any AWS login to browse a public bucket via the console) or the
  plain unauthenticated listing:
  `https://met-office-atmospheric-model-data.s3.eu-west-2.amazonaws.com/?list-type=2&delimiter=/&prefix=uk-deterministic-2km/`
- `FORCE_RERUN=1` (env var, also exposed as a `force_rerun` workflow_dispatch
  input on the "Fetch UKV Forecast" GitHub Action) bypasses the "already
  processed" guard so a boundary/aggregation fix can be reprocessed onto the
  latest run without waiting for the next one — but only once `is_run_complete`
  is also satisfied for that run.

## ECMWF HRES forecast fetch (`fetch_ecmwf.py`)

- Second, independent deterministic-rainfall page (`ecmwf.html`) modelled on
  UKV's pipeline, covering the same map domain/canvas for direct visual
  comparability. **Not sourced from ECMWF directly** — ECMWF's own free Open
  Data feed is capped at 0.25° (~28km), and a genuinely useful map needs
  something closer to the model's real resolution. Instead this pulls from
  **Open-Meteo's public AWS Open Data S3 bucket** (`s3://openmeteo`,
  `us-west-2`, unauthenticated), which redistributes ECMWF's own IFS HRES
  output at full **~9km native resolution** under the same CC BY 4.0 licence
  — ECMWF has said they'll offer this resolution directly "later in 2026",
  at which point revisit whether to switch source. Path:
  `data_spatial/ecmwf_ifs/{YYYY}/{MM}/{DD}/{HH}00Z/{valid_iso}.om`, read via
  the `omfiles` package (not GRIB/cfgrib — an earlier version of this script
  pulled ECMWF's own 0.25° GRIB2 files directly and smoothed them for
  display; that whole approach was replaced, not extended, once the 9km
  source was found and validated).
- The source grid is ECMWF's **O1280 octahedral reduced Gaussian grid** —
  unstructured (each latitude row has a different point count), with no
  coordinate array shipped per file since it's static across every timestep
  and run. `build_o1280_grid()` reconstructs it from first principles
  (Gaussian latitudes via Gauss-Legendre quadrature + the standard
  octahedral points-per-row formula `pl(i) = 4*i + 16`) — trust this only
  because it was verified two independent ways before being relied on: the
  computed point count matches Open-Meteo's array length exactly
  (6,599,680), and a rendered UK-domain subset lines up pixel-for-pixel with
  the UK coastline. If this ever needs re-deriving, re-run that same
  validation before trusting a change to it.
- Remapping onto the output canvas uses **inverse-distance-weighted
  interpolation over each pixel's 4 nearest source points**
  (`scipy.spatial.cKDTree`, built once per run in `build_mapping()` since the
  source grid never changes) — not a regular-grid method, because O1280 isn't
  a regular grid. This is what gives the page its smooth, ECMWF-chart-like
  rain boundaries instead of blocky grid cells.
- Open-Meteo's `precipitation` field is a genuine **backward sum** — the
  rainfall (mm) for the interval since the *previous* available valid time
  (1h/3h/6h depending on where in the run's cadence a step falls) — verified
  empirically (global sums scale with interval length, not cumulatively).
  Unlike raw ECMWF `tp` (cumulative since T+0), this needs **no
  de-accumulation step**: each step's value already is that interval's
  rainfall, fed straight into the accum-stack windowing logic UKV also uses.
- Same horizon split as ECMWF's own feed, just relabelled by run hour rather
  than a source-provided "stream" field: 00Z/12Z runs (`stream: "oper"`) go
  to the full **15-day** horizon (hourly to T+90h, 3-hourly to T+144h,
  6-hourly beyond); 06Z/18Z runs (`stream: "scda"`) only reach **6 days**
  (T+144h). `ecmwf.html` labels `scda` runs "(short-range)" in the run
  dropdown.
- Run completeness is read straight from each run's `meta.json`
  (`completed: true/false` + the exact `valid_times` list) — much simpler
  than the old GRIB2 approach's "probe for a specific final-step file"
  check, and avoids hardcoding a step schedule that could drift.
- v1 is deliberately lean vs UKV: one colour scheme (`_MET_COLORS`, the same
  "alternative" palette UKV itself offers, chosen so the two pages aren't
  visually confusable) and four accumulation windows (3h/6h/24h/48h, not
  UKV's six) — no splat masks and no gauge time series. R2 key layout
  mirrors UKV's: `ecmwf/{run_ts}/{offset}_{prefix}_met.png`.
- **Area averages** are published per step as
  `ecmwf_poly/{run_ts}/{offset}.json`, shaped exactly like UKV's
  `ukv_poly/`: `{boundary: {accum_key: {area_name: mm}}}`. They are computed
  from the *same* accumulation arrays the PNGs are rendered from, so the grid
  and `/ecmwf`'s Areas view can never disagree, and only for the three
  boundary sets that page offers (regions, counties, catchments) rather than
  all five in `GEOJSON_LAYERS`.
  - Masks are cached under **`ecmwf_masks/`, not `ukv_masks/`**. The two
    grids are byte-identical (both `-26..17`, `43..63`, 1725x1800) so the npz
    files really are duplicates, but sharing a prefix would couple the two
    scripts through the cached geojson fingerprint: change what either one
    puts in it and *both* pipelines silently rebuild masks on every run. The
    loader is the grid-agnostic `poly_utils.load_or_build_masks()`;
    `fetch_ukv.py` and `make_accum_multi.py` keep their own copies
    deliberately, since rewriting either for a feature about a third page is
    regression risk for no gain.
  - **Forward-only.** The script exits early when the newest complete run is
    already processed, and `FORCE_RERUN=1` only reprocesses that newest run,
    so runs already in the manifest when this shipped have no poly file and
    never will. `/ecmwf` shows a "not published for this frame" notice and a
    way back to the grid rather than an empty map. Same for windows a step is
    too early for — `accum_24h` before T+24 has no key.
  - Retention: `cleanup_old_ecmwf_runs()` purges `ecmwf_poly/{run_ts}/`
    alongside `ecmwf/{run_ts}/`, or the averages outlive their imagery.
- Higher time-resolution (hourly to T+90h) and a longer horizon (15 vs the
  old feed's 10 days) mean roughly 2.5x more steps/day than the original
  0.25°-based design — still only ~75k R2 ops/month (well within budget, see
  above), but worth knowing if step counts ever look surprising.
- Availability delay after nominal run time is roughly 6-9 hours (no
  additional delay vs ECMWF's own 0.25° feed, per Open-Meteo). The Google
  Apps Script trigger for the "Fetch ECMWF HRES Forecast" workflow should
  poll **hourly** — frequent enough to pick up each of the 4 daily runs
  within about an hour, while a "not ready yet" check is just a small
  `meta.json` fetch, so faster polling buys nothing.
- `ecmwf.html` shows catchment/region/county boundary outlines for
  geographic reference (same GeoJSON sources as `/ukv`), and fills them as a
  choropleth in its Areas view from the `ecmwf_poly/` files above.
- No GRIB/eccodes system dependency any more (`ecmwf.yml` no longer runs
  `apt-get install libeccodes-dev`) — reading `.om` files is pure
  Python+numpy+scipy, which simplified the workflow versus the first cut of
  this script.
- **Run time is latency-bound, not bandwidth-bound**: traced actual S3
  requests and found `omfiles` reads each file as ~40-50 small (~65KB)
  sequential range GETs rather than one bulk transfer — the real data
  needed per file is only ~1-2MB, so wall-clock is dominated by per-request
  network round-trip time, not data volume. That makes the per-step fetch
  step highly parallelizable, but threads alone hit a ceiling around 16
  concurrent (30 threads in one process measured *worse* than 16 — likely
  contention on fsspec's internal sync-over-async event loop).
- **Process-sharded fetch breaks past that ceiling**: `fetch_all_steps()` /
  `STEP_FETCH_PROCESSES` (4) x `STEP_FETCH_THREADS_PER_PROCESS` (8) in
  `fetch_ecmwf.py` — separate OS processes don't share the single-event-loop
  bottleneck threads do. Measured ~2x faster than 16 threads in one process
  against the same real files (82.1s -> ~41s for 24 files), and verified
  byte-identical output against the old single-process path before shipping
  (not just faster — provably correct). The large IDW remap arrays
  (`nn_idx`/`weights`, ~100MB each) are deliberately kept out of the worker
  processes — `fetch_and_subset()` runs in the worker and returns only the
  ~80k-point subset inside the output domain (small enough to pickle
  cheaply across the process boundary); `remap_subset()` does the actual
  IDW remap back in the main process, where those large arrays already
  live. Getting this split wrong (sending the full remap arrays to every
  worker) would have cost more in pickling overhead than the extra
  processes save — worth remembering if this is ever "optimized" further.
  If a future run still feels slow, re-measure before just raising either
  number — this is empirically tuned, not a guess, and there's a documented
  reason 30 threads and a naive multi-process split both underperform.

## Archived event replays (`backfill_compare_event.py` / `compare-early-september/`)

- `/compare` is a **nowcast** tool and everything it reads is on a short rolling
  window: `ukv/`, `ukv_poly/`, `ukv_gauge/`, `ukv_area_ts/` are purged by
  `cleanup_old_ukv_runs()` as soon as a run drops out of `ukv_meta.json`'s 72h
  list, and `accum_hist/`, `accum_poly/`, `gauge_bias/` expire at 14 days. So a
  week after any event, **the forecast half of the comparison no longer exists**
  and the observed half is on a clock. That is what this pipeline is for.
- `backfill_compare_event.py` rebuilds one event into `compare_events/{event_id}/`,
  a prefix **no cleanup job touches**. The two halves are not symmetric and the
  difference matters:
  - **Forecast half is regenerated.** Met Office's public S3 bucket keeps its UKV
    NetCDF far longer than we keep our renders (verified back to Sep 2024), so
    the runs are re-downloaded and re-rendered through `fetch_ukv.py`'s own
    functions. Not urgent — it can be redone at any time.
  - **Observed half is copied, and this is time-critical.** Radar accumulations
    are built from frames we no longer hold, so once `accum_hist/` expires at 14
    days that data is gone for good. **Snapshot an event's radar side within 14
    days or it cannot be archived at all** (`--mode radar`, and it is the first
    job in the workflow for exactly this reason).
- The only change to `fetch_ukv.py` is an optional `base=` key prefix on
  `upload_schemes()`, defaulting to the live `"ukv"`. The per-step render loop is
  deliberately **duplicated** in the backfill script rather than factored out of
  `main()`: `main()` is the live pipeline running every three hours, and
  reshaping it around a one-off replay is regression risk for no operational gain.
- Events are declared in that script's `EVENTS` table (run range, observed
  window, plus `event_start`/`event_end`/`focus_ts`/`default_run`, which let the
  page open *on* the event instead of at the end of the archive window). Adding
  another past event is a new entry there plus a copy of the page — no new
  plumbing.
- Run it from the **Backfill Compare Event** workflow (`workflow_dispatch` only).
  `--mode ukv` shards across 6 matrix jobs; sharding is round-robin so the
  03Z/15Z runs — which publish a 3-hourly tail past T+54h and so carry ~40% more
  steps — spread evenly instead of piling into one long shard. Every mode is
  idempotent: a re-run after a partial failure costs Class B ops, not Class A.
  The `meta` job runs `if: always()` and assembles from whatever fragments are
  actually on R2, so one failed shard means a manifest missing that run rather
  than no page at all.
- Cost, for reference when adding an event: ~1,000 forecast steps for a 17-run
  event is roughly 31k R2 Class A ops, plus ~5.8k for the radar copy — about 4%
  of the monthly free tier (see the R2 budget section above), one-off.
- `compare-early-september/index.html` is a fork of `compare.html`, not a
  refactor of it — the live page keeps working unchanged. It differs only in:
  loading one static `meta.json` instead of the two live manifests (and so no
  polling loop), prefixing the per-frame JSON side-loads with `EVENT_BASE`,
  `../` on repo-root assets since it sits one directory down, an identity strip
  plus a shaded event span on the timeline, **times in GMT throughout**, and a
  **panel order of forecast, observation, bias** (`/compare` leads with the
  observation). The panes are addressed by id everywhere in the JS, so that
  reorder is DOM-only — but the Leaflet zoom control has to be moved with it, or
  it renders in the middle of the row instead of at its left edge.
- **Event pages are GMT/UTC end to end — do not "fix" this to local time.**
  The timeline readout, the run dropdown, the per-step `valid_label`, the radar
  snapshot timestamps and the popup chart axis are all UTC stamped `GMT`. That
  is the convention UK meteorology works in and what makes an archive readable
  as a record. It takes a deliberate departure from the live pipeline: the run
  and step labels in the manifest come from `event_run_label()` in
  `backfill_compare_event.py`, **not** `fetch_ukv.run_label_str()` /
  `valid_label_str()`, which localise to UK time via `_uk_local()` and would
  print an event inside BST an hour ahead of every raw timestamp beside it. If
  a future event page shows a run dropdown an hour out from its slider, this is
  why: something has been routed back through the live label helpers.

## FGS tracker (`fetch_fgs.py` / `fgscomparison/index.html`)

- FGS = the FFC 5-day Flood Guidance Statement (daily ~10:30 UK, occasional
  amendments). API: `https://api.ffc-environment-agency.fgs.metoffice.gov.uk/api/public/v3/statements`
  (header `X-Api-Key`, secret `FFC_API_KEY`/`FGS_API_KEY`; same host as RFG).
  The API keeps only the ~50 newest statements — our archive accumulates them
  permanently on R2.
- Each statement links a `detailed_csv_url` ("five day export"): one row per
  county per day (109 England & Wales counties × 5 days) with overall risk
  (VL/L/M/H) plus a two-digit cell per source (rivers/surface/coastal/ground):
  first digit impact 1–4, second likelihood 1–4, comma-separated when a county
  has multiple areas of concern (e.g. `32,33`). `14` (minimal impact, high
  likelihood) is the everywhere-default "nothing to see" value.
- R2 keys: `fgs/index.json` (rolling index with per-day above-VL and
  areas-of-concern county counts) and `fgs/archive/{id}.json` +
  `fgs/archive/{id}_aoc.jpg` (parsed statement + archived area-of-concern
  image). Statements are re-fetched when `last_modified_at` changes.
  Workflow: `fgs_update.yml`, hourly cron.
- `fgscomparison/index.html` compares successive statements **by valid date**
  (day 2 of yesterday's issue = day 1 of today's): change lists, county
  choropleths (matched to `geo/uk-counties.geojson` by expanding N/S/E/W/NE/Gtr
  abbreviations), diff map, per-source 4×4 risk matrices, county × date table.
  Overall county risk often stays VL while all the action is in source cells,
  so change detection always scans every source, not just the selected view.

## Named storm archive (`fetch_storms.py` / `storm_scraper.py` / `storm_summaries.py`)

- Migrated here from the sibling **floodforecast** Flask app's
  `/trigger-storm-update` route because this repo already
  had the `OPENAI_API_KEY`/`OPENAI_MODEL` secret and the
  workflow_dispatch-triggered-by-Google-Apps-Script convention the job needs —
  floodforecast had neither. Triggered externally roughly every 12 hours via
  the `storms_update.yml` workflow (`workflow_dispatch` only, no cron, same as
  every other job here). floodforecast's `/trigger-storm-update` route still
  exists and still works — it was *not* removed when the scheduled job moved
  here, and an earlier version of this note wrongly said it had been. Treat
  this repo as the scheduled runner and that route as a manual fallback; just
  don't point a scheduler at both, or two writers race on the same
  `storms.csv`.
- **This is the only script in the repo that writes to Google Cloud Storage**
  rather than R2. `storms.csv` lives in floodforecast's own GCS bucket
  (`grounded-chain-373213.appspot.com`) because its `/storms` page reads it
  from there directly — moving the *scraper* here doesn't move the *data*.
  `gcs_storms.py` is a minimal read/write helper for just this file, using env
  var `GOOGLE_JSON_KEY_FLOODFORECAST` — the same GCP service-account JSON key
  floodforecast's own `gcs_utils.py` already uses under that name, copied into
  a radar secret rather than a new key. Env-var-only, no local-file/ADC
  fallback (meaningless on a runner) — a missing secret fails loudly rather
  than trying some other auth path.
- `storm_scraper.py` (parses the Met Office Storm Centre — no API exists) and
  `storm_summaries.py` (LLM summaries from the storm's report PDF and/or the
  NSWWS warnings archive below) are verbatim ports from floodforecast's copies
  of the same files. floodforecast keeps its own copies as a manual
  dev-machine fallback — if you fix a parsing hazard here (the module
  docstrings list several real ones already hit: `&nbsp;` reserved names,
  non-ASCII names, cross-month date ranges, one PDF covering two storms), port
  the fix to both.
- `fetch_storms.py`'s `update_storms()` is a **merge, not an insert**: storm
  information arrives gradually (name first, impact dates a day or two later,
  a report PDF weeks later or never), so a field already in `storms.csv` is
  never overwritten with a blank. Storms are keyed by name **and** season —
  names repeat across seasons (Hannah in 2018-19 and 2025-26).
- `_load_warnings_for_window()` reads Met Office warnings back from **this
  repo's own public R2 CDN** (`warnings/archive/warnings_YYYYMMDD.json`) as
  evidence for the LLM summary — a same-repo HTTP round trip rather than
  reading local files, kept that way for parity with the floodforecast copy.
  Surface obs are deliberately not used as wind evidence: `surface_obs`
  archives 1-minute *mean* wind with no gust field, and quoting a mean as a
  storm's peak gust would read as plainly wrong.
- `python fetch_storms.py storms --dry-run` runs the full scrape/merge and
  prints what would change without writing to GCS — there's no test bucket,
  so this is the only safety net before a real write against the CSV
  floodforecast.co.uk reads live.

## Gauge QC (`gauge_qc.py` / `gaugecheck.html`)

- Results: `gaugecheck/results.json` (current run snapshot)
- Log: `gaugecheck/log.json` (rolling 48 h event log)
- Manifest + run archive: `gaugecheck/manifest.json` + `gaugecheck/runs/*.json`
- The log accumulates events keyed by `(station_id, first_flagged_at)` — one entry per station per "flagging episode".

## Rainfall duration records & percentiles (`rain_duration_stats.py` / `rain_geo.py`)

- Adds "how unusual is this total" context to `rain/gauge.html`'s duration
  table for 1h/3h/6h/12h/24h/48h/72h/7d, split into two genuinely different
  kinds of record because **no sub-daily historical archive exists anywhere
  upstream** (EA's bulk archive, MIDAS Open, and this repo's own 15-min
  readings are all daily-or-coarser once you look past `fetch_rain.py`'s
  14-day retention window):
  - **24h/48h/72h/7d — `basis.kind:"climatological"`**, decades deep.
    `build_rain_climatology.py`'s `compute_duration_climatology()` (called
    from inside `compute_climatology()`, since the raw daily `days` dict is
    discarded right after — see that function's own comment) computes
    rolling N-day sums from the same daily series already pulled for the
    monthly climatology, gated on `MIN_DAYS_FOR_DURATION_PERCENTILE=365`
    windows before publishing percentiles. `build_sepa_climatology.py` and
    `build_nrw_climatology.py` inherit this for free (same shared
    function) — NRW's result is explicitly re-tagged `basis.kind:"proxy"`
    afterwards, since it's still MIDAS-matched data, not the gauge's own.
  - **1h/3h/6h/12h — `basis.kind:"operational"`**, growing from whenever
    this feature shipped. No history exists or can be recovered, so
    `rain_duration_stats.py`'s `update_duration_records()` forward-tracks an
    all-time max per station per duration from live rolling totals every
    15-min run (`make_rain_summary.py:main()`), with a plausibility ceiling
    (`CEILING_MM`, shared with the 24h ceiling that also fixed a
    long-standing 250mm-vs-341.4mm inconsistency between
    `make_rain_summary.py` and `rain/gauge.html`) and a lightweight
    neighbour-corroboration check (deliberately independent of
    `gauge_qc.py`'s own MAD-based spatial check — decoupled pipelines) that
    holds an implausible or spatially-uncorroborated candidate as
    `provisional` rather than either trusting or discarding it outright.
  - Every stat carries a `basis` object (`climatological` / `operational` /
    `proxy`, with a `shortRecord` flag) — one shared shape, not a fourth ad
    hoc way of flagging thin history.
- R2 keys: `rain/duration_climatology_{ea,sepa,nrw}.json` (slim per-network
  percentile+max index, loaded once per run rather than one GET per station
  at 15-min cadence — see CLAUDE.md's R2 budget rules above),
  `rain/duration_records.json` (hot, consolidated, one file not one per
  station — same rule), `rain/duration_peaks_{YYYY}.json` (cold, one entry
  appended per station per duration per day, never pruned — same precedent
  as `rain/archive/daily_{YYYY}.json`), `rain/duration_stats.json`
  (cross-gauge rank/percentile context, kept separate from `rain/summary.json`
  since `rain/table.html` fetches that file on every load and doesn't need
  this data), `rain/neighbours.json` (cached k-nearest-neighbour index,
  rebuilt only when `stations.json`'s coordinates change).
- `rain/gauge.html`'s duration table shows only the "obvious first cut" —
  a record pill / "X% of record" and a rank-on-record rarity line (not a raw
  percentile: with decades of daily data almost every meaningful rain event
  already sits above the 99th percentile, so a literal percentile number
  would be simultaneously alarmist and uninformative). Cross-gauge
  percentiles (`rankUk`/`rankNeighbours`/`neighboursRecordsExceeded`) are
  computed and stored in `rain/duration_stats.json` but deliberately not
  built into the UI yet — comprehensive analysis layer, UI catches up later.
  NRW gauges don't show the new columns at all (matches the existing refusal
  to show NRW's proxy daily climatology).
- **Backfilling onto already-processed stations**: the duration-climatology
  addition needed a way to reprocess stations already in
  `climatology_index_{ea,sepa,nrw}.json` without a blanket `FORCE=1` re-pull
  of every station's full history — `DUR_SCHEMA_VERSION` in
  `build_rain_climatology.py`, stamped per-station as `durSchema` in the
  index, does this: a ref is reprocessed if missing OR its `durSchema`
  doesn't match. Bump `DUR_SCHEMA_VERSION` for any future change to
  `compute_duration_climatology()`'s output shape.
- **Ready-made alerting hook**: detecting "gauge X just broke a Nh record" is
  a one-line diff of `rain/duration_records.json`'s `allTime.at` timestamp
  between runs — the file is deliberately shaped so a future alert job can
  consume it directly without touching this pipeline.
- **Deliberately not pursued** (see the implementation plan for the full
  reasoning): MIDAS's separate sub-hourly rainfall dataset, which could in
  principle give 1h-12h durations genuine historical depth too, needs its
  own access/licensing investigation; and FEH13 return-period estimates
  (the actual UK forecaster standard for "how unusual"), which need CEH's
  commercial WINFAP-FEH tooling or an unclear-licence web service. The
  rank-on-record framing here is a deliberately honest, licence-clean
  substitute for either, not a permanent final answer.
