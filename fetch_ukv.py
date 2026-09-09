"""
fetch_ukv.py — Download Met Office UKV 2km forecast rainfall, render PNGs, upload to R2.

Met Office S3 bucket: met-office-atmospheric-model-data (eu-west-2, public)
Run prefix: uk-deterministic-2km/YYYYMMDDTHHMMZ/
File naming: {RUNTIME}-{OFFSET}-{PARAMETER}.nc

Usage: python fetch_ukv.py
Required env: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
              R2_BUCKET_NAME, R2_PUBLIC_BASE_URL
"""
import io
import json
import os
import re
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3
import numpy as np
import xarray as xr
from botocore import UNSIGNED
from botocore.client import Config
from PIL import Image
from pyproj import CRS, Transformer

from poly_utils import (
    GEOJSON_LAYERS,
    build_merc_axes,
    build_pixel_masks,
    masks_to_npz_bytes,
    npz_bytes_to_masks,
    compute_poly_averages,
    geojson_fingerprint,
)

# ── Output domain — covers full UKV LAEA extent (valid mask handles transparency)
LON_MIN, LON_MAX = -26.0, 17.0
LAT_MIN, LAT_MAX =  43.0, 63.0
WIDTH,   HEIGHT  =  1725, 1800

# ── Met Office S3 ────────────────────────────────────────────────────────────────
MET_BUCKET = "met-office-atmospheric-model-data"
MET_REGION = "eu-west-2"
UKV_PREFIX = "uk-deterministic-2km/"

# ── Colour schemes ───────────────────────────────────────────────────────────────
# Same colours and bounds as /radar (make_accum_multi.py) so /ukv and /radar are
# directly comparable, regardless of which accumulation duration is selected.
_STD = np.array([
    [  0,   0,   0,   0],   # transparent (index 0 — no rain)
    [224, 243, 255, 255],
    [129, 212, 250, 255],
    [  3, 169, 244, 255],
    [  1,  87, 155, 255],
    [ 46, 125,  50, 255],
    [251, 192,  45, 255],
    [239, 108,   0, 255],
    [198,  40,  40, 255],
    [106,  27, 154, 255],
], dtype=np.uint8)

_MET_COLORS = np.array([
    [  0,   0,   0,   0],   # transparent  < 0.03
    [ 58, 108, 255, 255],   # blue          0.03–1
    [  0, 255,   0, 255],   # bright green  1–5
    [255, 255, 149, 255],   # pale yellow   5–10
    [255, 213,  99, 255],   # sand          10–20
    [255, 150,  24, 255],   # orange        20–40
    [232,  97,   0, 255],   # dark orange   40–60
    [186,  32,   0, 255],   # red           60–80
    [204,  83, 125, 255],   # rose          80–100
    [219, 146, 220, 255],   # light purple  100–120
    [255,   2, 255, 255],   # magenta       120–140
    [255, 255, 255, 255],   # white         140–160
    [200, 200, 200, 255],   # light grey    160–180
    [191, 191,   0, 255],   # olive         > 180
], dtype=np.uint8)

# Rate schemes (mm/hr)
RATE_SCHEMES = {
    "norm": {"bounds": np.array([0.1, 0.5, 1, 2, 4, 8, 16, 32, 64]),    "colors": _STD},
    "high": {"bounds": np.array([0.5, 2,   4, 8, 16, 32, 64, 100, 150]), "colors": _STD},
    "met":  {"bounds": np.array([0.03, 0.1, 0.5, 1, 2, 4, 8, 16, 32, 64, 100, 150, 200]), "colors": _MET_COLORS},
}

# Accumulation schemes (mm) — shared across all durations, mirrors /radar.
ACCUM_SCHEMES = {
    "norm": {"bounds": np.array([0.5,  1,   2,   5,  10,  20,  40,  80,  160]),
             "colors": _STD},
    "high": {"bounds": np.array([2,    5,  10,  25,  50, 100, 150, 200,  350]),
             "colors": _STD},
    "met":  {"bounds": np.array([0.03, 1,   5,  10,  20,  40,  60,  80, 100, 120, 140, 160, 180]),
             "colors": _MET_COLORS},
}

ACCUM_PERIODS = [1, 3, 6, 12, 24, 48]

# Splat map thresholds matching /obs + 12h test threshold (mm)
SPLAT_THRESHOLDS = {
    "1h": [10.0, 30.0],
    "3h": [20.0, 30.0, 40.0],
    "6h": [30.0, 50.0],
    "12h": [1.0],
}

# ── R2 ───────────────────────────────────────────────────────────────────────────
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET     = os.environ.get("R2_BUCKET_NAME", "")
R2_PUBLIC_URL = os.environ.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
USE_R2 = all([R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET, R2_PUBLIC_URL])


def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


# Thread-local R2 client so upload threads each hold their own connection.
_tl = threading.local()

def _r2_tl():
    if not getattr(_tl, "r2", None):
        _tl.r2 = get_r2()
    return _tl.r2


def get_s3():
    return boto3.client("s3", region_name=MET_REGION,
                        config=Config(signature_version=UNSIGNED))


def json_from_r2(r2, key, default=None):
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def json_to_r2(r2, key, data):
    r2.put_object(Bucket=R2_BUCKET, Key=key,
                  Body=json.dumps(data, indent=2).encode(),
                  ContentType="application/json; charset=utf-8")


def png_to_r2(r2, key, img):
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    r2.put_object(Bucket=R2_BUCKET, Key=key,
                  Body=buf.getvalue(), ContentType="image/png")


# ── Run discovery ─────────────────────────────────────────────────────────────────
_STANDARD_HOURS = {0, 3, 6, 9, 12, 15, 18, 21}

def _is_standard_run(run_ts):
    """True if the run is at a standard 3-hourly init time (00/03/06/09/12/15/18/21Z)."""
    try:
        return parse_run_dt(run_ts).hour in _STANDARD_HOURS
    except Exception:
        return False


def find_latest_run(s3):
    """Find the latest run that is fully complete (T+54h data available).

    Files are named {valid_time}-{offset}-{param}.nc (not {run_time}-...).
    List run folders by date (today first) and walk newest-to-oldest. A run
    that has started publishing (T+1h present) but hasn't reached T+54h yet
    is still mid-publish — rather than returning it and giving up, keep
    looking further back so a slow/delayed newest run doesn't stall
    processing of an older run that already finished completely.
    """
    now = datetime.utcnow()
    newest_seen = None
    for days_back in range(7):
        date = (now - timedelta(days=days_back)).strftime("%Y%m%d")
        resp = s3.list_objects_v2(
            Bucket=MET_BUCKET,
            Prefix=f"{UKV_PREFIX}{date}",
            Delimiter="/",
        )
        all_folders = [p["Prefix"].rstrip("/").split("/")[-1] for p in resp.get("CommonPrefixes", [])]
        # Only consider runs at standard 3-hourly init times (00, 03, 06, 09, 12, 15, 18, 21Z)
        run_folders = sorted(
            [f for f in all_folders if _is_standard_run(f)],
            reverse=True,
        )
        print(f"  {date}: {len(run_folders)} standard run folders ({len(all_folders)} total)")
        for run_ts in run_folders:
            run_dt   = parse_run_dt(run_ts)
            valid_dt = run_dt + timedelta(hours=1)
            valid_ts = valid_dt.strftime("%Y%m%dT%H%MZ")
            prefix   = f"{UKV_PREFIX}{run_ts}/{valid_ts}-PT0001H00M-rainfall_rate.nc"
            chk = s3.list_objects_v2(Bucket=MET_BUCKET, Prefix=prefix, MaxKeys=1)
            if not chk.get("Contents"):
                print(f"    {run_ts}: T+1h not available yet")
                continue
            if newest_seen is None:
                newest_seen = run_ts
            if is_run_complete(s3, run_ts):
                return run_ts
            print(f"    {run_ts}: T+1h present but run not yet complete (no T+54h) — checking earlier run")
    if newest_seen:
        print(f"  No complete run found; newest in-progress run is {newest_seen}")
    return None


def is_run_complete(s3, run_ts):
    """Return True only when the run's final expected forecast step (T+54h) is on S3."""
    run_dt      = parse_run_dt(run_ts)
    final_hours = 54
    final_off   = f"PT{final_hours:04d}H00M"
    valid_ts    = (run_dt + timedelta(hours=final_hours)).strftime("%Y%m%dT%H%MZ")
    prefix      = f"{UKV_PREFIX}{run_ts}/{valid_ts}-{final_off}-rainfall_rate.nc"
    chk = s3.list_objects_v2(Bucket=MET_BUCKET, Prefix=prefix, MaxKeys=1)
    return bool(chk.get("Contents"))


def parse_run_dt(run_ts):
    for fmt in ("%Y%m%dT%H%MZ", "%Y%m%dT%HZ"):
        try:
            return datetime.strptime(run_ts, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse run timestamp: {run_ts}")


def _uk_local(dt_utc):
    """Return (local_dt, tz_name) for UK time (BST or GMT), no external deps.
    Mirrors make_agent_summary.py's helper of the same name — kept as a
    separate copy per this repo's convention of independent single-file
    scripts rather than a shared util module."""
    import calendar
    year = dt_utc.year
    def last_sunday(y, month):
        last_day = calendar.monthrange(y, month)[1]
        d = datetime(y, month, last_day, 1, 0, tzinfo=timezone.utc)
        while d.weekday() != 6:
            d -= timedelta(days=1)
        return d
    bst_start = last_sunday(year, 3)
    bst_end   = last_sunday(year, 10)
    if bst_start <= dt_utc < bst_end:
        return dt_utc + timedelta(hours=1), "BST"
    return dt_utc, "GMT"


def run_label_str(run_ts):
    # parse_run_dt returns a naive UTC datetime — localize to UK time (BST/GMT)
    # before formatting, rather than always stamping the raw UTC clock value
    # with a hardcoded "GMT" suffix (which was wrong for half the year).
    local, tz = _uk_local(parse_run_dt(run_ts).replace(tzinfo=timezone.utc))
    return f"{local.day} " + local.strftime("%b %Y %H:%M") + f" {tz}"



# ── Step discovery ────────────────────────────────────────────────────────────────
WHOLE_HOUR_RE = re.compile(r"^PT(\d{4})H00M$")
# rainfall_rate may only cover T+1..T+36/38; accumulation typically extends further.
# Discover from either so we don't miss the longer tail of the forecast.
_DISCOVER_SUFFIXES = ("-rainfall_rate.nc", "-rainfall_accumulation-PT01H.nc")


def discover_steps(s3, run_ts):
    """Return list of (offset_hours, offset_str, valid_ts) for whole-hour forecast steps.

    Filenames: {valid_time}-{offset}-{param}.nc — valid_time is NOT the run time.
    Uses both rainfall_rate and rainfall_accumulation files for step discovery so
    that extended forecast periods (e.g. T+39–T+54) are not missed if rainfall_rate
    is only archived for the short range.
    """
    prefix = f"{UKV_PREFIX}{run_ts}/"
    steps_dict, cont = {}, None
    while True:
        kwargs = {"Bucket": MET_BUCKET, "Prefix": prefix}
        if cont:
            kwargs["ContinuationToken"] = cont
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not any(key.endswith(s) for s in _DISCOVER_SUFFIXES):
                continue
            fname = os.path.basename(key)
            parts = fname.split("-")
            if len(parts) < 3:
                continue
            valid_ts   = parts[0]   # e.g. "20260514T0700Z"
            offset_str = parts[1]   # e.g. "PT0001H00M"
            m = WHOLE_HOUR_RE.match(offset_str)
            if not m:
                continue
            hours = int(m.group(1))
            if hours == 0:
                continue
            if hours not in steps_dict:
                steps_dict[hours] = (offset_str, valid_ts)
        if resp.get("IsTruncated"):
            cont = resp["NextContinuationToken"]
        else:
            break

    steps = [(h, o, v) for h, (o, v) in steps_dict.items()]
    steps.sort()
    return steps


# ── Coordinate mapping (built once per run) ──────────────────────────────────────
NETCDF_LOCK = threading.Lock()


def build_mapping(nc_path):
    """Return (rows, cols, valid) arrays for remapping UKV grid → output Mercator grid."""
    with NETCDF_LOCK:
        with xr.open_dataset(nc_path, engine="netcdf4") as ds:
            # Find the grid_mapping variable (scalar variable with grid_mapping_name attr)
            gm_var = None
            for v in list(ds.data_vars) + list(ds.coords):
                if "grid_mapping_name" in ds[v].attrs:
                    gm_var = v
                    break
            if gm_var is None:
                raise ValueError("No grid_mapping variable found in NetCDF")

            attrs   = dict(ds[gm_var].attrs)
            gm_name = attrs.get("grid_mapping_name", "")
            print(f"    grid_mapping: {gm_name}")

            if gm_name == "rotated_latitude_longitude":
                pole_lat = float(attrs["grid_north_pole_latitude"])
                pole_lon = float(attrs["grid_north_pole_longitude"])
                print(f"    rotated north pole: lat={pole_lat}, lon={pole_lon}")
                src_crs = CRS.from_cf({
                    "grid_mapping_name": "rotated_latitude_longitude",
                    "grid_north_pole_latitude":  pole_lat,
                    "grid_north_pole_longitude": pole_lon,
                })
                src_lats = src_lons = None
                for c in ["grid_latitude", "rlat", "latitude"]:
                    if c in ds.coords and ds[c].ndim == 1:
                        src_lats = ds[c].values
                        print(f"    src_lats from '{c}': {len(src_lats)} pts [{src_lats[0]:.4f}..{src_lats[-1]:.4f}]")
                        break
                for c in ["grid_longitude", "rlon", "longitude"]:
                    if c in ds.coords and ds[c].ndim == 1:
                        src_lons = ds[c].values
                        print(f"    src_lons from '{c}': {len(src_lons)} pts [{src_lons[0]:.4f}..{src_lons[-1]:.4f}]")
                        break
            elif gm_name in ("transverse_mercator", "lambert_azimuthal_equal_area"):
                src_crs  = CRS.from_cf(attrs)  # build from file attrs, not EPSG:27700
                src_lats = src_lons = None
                for c in ["projection_y_coordinate", "northing", "y"]:
                    if c in ds.coords and ds[c].ndim == 1:
                        src_lats = ds[c].values
                        print(f"    src_lats(y) from '{c}': {len(src_lats)} pts [{src_lats[0]:.1f}..{src_lats[-1]:.1f}]")
                        break
                for c in ["projection_x_coordinate", "easting", "x"]:
                    if c in ds.coords and ds[c].ndim == 1:
                        src_lons = ds[c].values
                        print(f"    src_lons(x) from '{c}': {len(src_lons)} pts [{src_lons[0]:.1f}..{src_lons[-1]:.1f}]")
                        break
            else:
                raise ValueError(f"Unsupported grid_mapping_name: {gm_name!r}")

    if src_lats is None or src_lons is None:
        raise ValueError("Could not identify source coordinate arrays")

    # Build output Mercator pixel grid
    ll_to_merc = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    merc_to_ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    ll_to_src  = Transformer.from_crs("EPSG:4326", src_crs,     always_xy=True)

    merc_xmin, merc_ymin = ll_to_merc.transform(LON_MIN, LAT_MIN)
    merc_xmax, merc_ymax = ll_to_merc.transform(LON_MAX, LAT_MAX)
    merc_xs = np.linspace(merc_xmin, merc_xmax, WIDTH)
    merc_ys = np.linspace(merc_ymax, merc_ymin, HEIGHT)  # top → bottom
    MX, MY  = np.meshgrid(merc_xs, merc_ys)

    # Two-step: Mercator → WGS84 → source CRS
    # (direct 3857→rotated CRS is unreliable in pyproj)
    Lon, Lat   = merc_to_ll.transform(MX, MY)
    Xsrc, Ysrc = ll_to_src.transform(Lon, Lat)

    print(f"    Xsrc range: [{np.nanmin(Xsrc):.4f}, {np.nanmax(Xsrc):.4f}]")
    print(f"    Ysrc range: [{np.nanmin(Ysrc):.4f}, {np.nanmax(Ysrc):.4f}]")

    x_step = float(src_lons[1] - src_lons[0])
    y_step = float(src_lats[1] - src_lats[0])
    cols   = np.round((Xsrc - float(src_lons[0])) / x_step).astype(np.int32)
    rows   = np.round((Ysrc - float(src_lats[0])) / y_step).astype(np.int32)
    valid  = (np.isfinite(Xsrc) & np.isfinite(Ysrc) &
              (rows >= 0) & (rows < len(src_lats)) &
              (cols >= 0) & (cols < len(src_lons)))

    print(f"    valid pixels: {valid.sum():,} / {valid.size:,} ({100*valid.mean():.1f}%)")
    return rows, cols, valid


# ── Data extraction ───────────────────────────────────────────────────────────────
def extract_array(nc_path, mapping):
    """Extract the main data variable, convert units, remap to output grid."""
    rows, cols, valid = mapping
    with NETCDF_LOCK:
        with xr.open_dataset(nc_path, engine="netcdf4") as ds:
            gm_names = {v for v in list(ds.data_vars) + list(ds.coords)
                        if "grid_mapping_name" in ds[v].attrs}
            candidates = [v for v in ds.data_vars
                          if v not in gm_names and ds[v].ndim >= 2]
            if not candidates:
                return None
            da    = ds[candidates[0]]
            arr2d = da.values.squeeze()
            units = (da.attrs.get("units", "") or "").lower()

    while arr2d.ndim > 2:
        arr2d = arr2d[0]
    if arr2d.ndim != 2:
        return None

    arr2d = np.where(np.isfinite(arr2d), arr2d, 0.0).astype(np.float32)
    arr2d = np.clip(arr2d, 0, None)

    pre_max = arr2d.max()
    u = units.strip()
    # Rate → mm/hr
    if "kg" in u and ("s-1" in u or "s**-1" in u):
        arr2d *= 3600.0
    elif u in ("m s-1", "m s**-1"):
        arr2d *= 3_600_000.0
    # Accumulation (m of water depth) → mm
    elif u == "m":
        arr2d *= 1000.0
    # kg m-2 (= mm) and dimensionless: no conversion

    out = np.zeros((HEIGHT, WIDTH), dtype=np.float32)
    out[valid] = arr2d[rows[valid], cols[valid]]
    return out


def render_png(arr, scheme):
    indices = np.digitize(arr, scheme["bounds"])
    indices[arr <= 0] = 0
    return Image.fromarray(scheme["colors"][indices], "RGBA")


def render_splat_png(arr, threshold):
    """Render binary mask PNG where arr >= threshold is solid white (RGBA 255,255,255,255), else transparent."""
    mask = arr >= threshold
    rgba = np.zeros((arr.shape[0], arr.shape[1], 4), dtype=np.uint8)
    rgba[mask] = [255, 255, 255, 255]
    return Image.fromarray(rgba, "RGBA")


# ── S3 download → temp file ───────────────────────────────────────────────────────
def download_nc(s3, run_ts, valid_ts, offset, param):
    """Key format: {prefix}{run_ts}/{valid_ts}-{offset}-{param}.nc"""
    key = f"{UKV_PREFIX}{run_ts}/{valid_ts}-{offset}-{param}.nc"
    try:
        obj = s3.get_object(Bucket=MET_BUCKET, Key=key)
        return obj["Body"].read()
    except Exception as e:
        print(f"    Warning: {os.path.basename(key)} — {e}")
        return None



def upload_schemes(arr, schemes, run_ts, offset, prefix, base="ukv"):
    """Render arr with each scheme, upload PNGs in parallel, return {scheme_name: url}.

    `base` is the R2 key prefix the PNGs are written under. It defaults to the
    live "ukv" prefix; backfill_compare_event.py passes its own event prefix so
    replayed historical runs land outside the tree cleanup_old_ukv_runs() prunes.
    """
    if arr is None:
        return None

    def _upload_one(sname, scheme):
        img = render_png(arr, scheme)
        key = f"{base}/{run_ts}/{offset}_{prefix}_{sname}.png"
        png_to_r2(_r2_tl(), key, img)
        return sname, f"{R2_PUBLIC_URL}/{key}"

    urls = {}
    with ThreadPoolExecutor(max_workers=len(schemes)) as ex:
        for sname, url in ex.map(lambda item: _upload_one(*item), schemes.items()):
            urls[sname] = url
    return urls


def _download_nc_bytes(s3, run_ts, valid_ts, offset, param):
    """Download a single NetCDF from Met Office S3, return raw bytes or None."""
    key = f"{UKV_PREFIX}{run_ts}/{valid_ts}-{offset}-{param}.nc"
    try:
        obj = s3.get_object(Bucket=MET_BUCKET, Key=key)
        return obj["Body"].read()
    except Exception as e:
        print(f"    Warning: {os.path.basename(key)} — {e}")
        return None


def load_arr_both(s3, run_ts, valid_ts, offset, mapping):
    """Download rainfall_rate and rainfall_accumulation-PT01H in parallel.

    Returns (arr_rate, arr_accum_1h) — either may be None.
    """
    params = [
        ("rainfall_rate", "rate"),
        ("rainfall_accumulation-PT01H", "accum"),
    ]

    results = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(_download_nc_bytes, s3, run_ts, valid_ts, offset, p): label
                for p, label in params}
        for fut in as_completed(futs):
            label = futs[fut]
            nc_bytes = fut.result()
            if nc_bytes is None:
                results[label] = None
                continue
            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                tmp.write(nc_bytes)
                path = tmp.name
            try:
                arr = extract_array(path, mapping)
                if arr is not None:
                    print(f"      [{label}] max={arr.max():.4f}  nonzero={np.count_nonzero(arr)}")
                results[label] = arr
            finally:
                os.unlink(path)

    return results.get("rate"), results.get("accum")


# ── Polygon masks (UKV grid) ──────────────────────────────────────────────────────
UKV_MASKS_PREFIX = "ukv_masks"


def load_or_build_ukv_masks(r2):
    """Load cached UKV pixel masks from R2, or rasterise and cache if absent.

    UKV output grid (1725×1800, lon -26..17, lat 43..63) differs from the radar
    accum grid so we maintain a separate mask cache under ukv_masks/.
    Returns {layer_name: {poly_name: (rows, cols)}}.
    """
    ll_to_merc, xmin, xmax, ymin_m, ymax_m = build_merc_axes(
        WIDTH, HEIGHT, LON_MIN, LON_MAX, LAT_MIN, LAT_MAX
    )

    all_masks = {}
    for layer_name, (geojson_path, name_key) in GEOJSON_LAYERS.items():
        if not os.path.exists(geojson_path):
            print(f"  [ukv_masks/{layer_name}] {geojson_path} not found — skipping")
            continue

        npz_key   = f"{UKV_MASKS_PREFIX}/{layer_name}.npz"
        names_key = f"{UKV_MASKS_PREFIX}/{layer_name}_names.json"
        fp_key    = f"{UKV_MASKS_PREFIX}/{layer_name}_fp.json"

        current_fp = geojson_fingerprint(geojson_path)

        # Try loading from R2 cache, but only if the source geojson hasn't changed
        try:
            fp_obj = r2.get_object(Bucket=R2_BUCKET, Key=fp_key)
            cached_fp = json.loads(fp_obj["Body"].read())
            if cached_fp != current_fp:
                raise ValueError("stale mask cache — source geojson changed")
            names_obj = r2.get_object(Bucket=R2_BUCKET, Key=names_key)
            names     = json.loads(names_obj["Body"].read())
            npz_obj   = r2.get_object(Bucket=R2_BUCKET, Key=npz_key)
            masks     = npz_bytes_to_masks(npz_obj["Body"].read(), names)
            print(f"  [ukv_masks/{layer_name}] loaded from R2 ({len(names)} polygons)")
            all_masks[layer_name] = masks
            continue
        except Exception:
            pass

        # Build from GeoJSON and cache
        print(f"  [ukv_masks/{layer_name}] building from {geojson_path}...")
        masks = build_pixel_masks(geojson_path, name_key, WIDTH, HEIGHT,
                                  ll_to_merc, xmin, xmax, ymin_m, ymax_m)
        names = list(masks.keys())
        print(f"  [ukv_masks/{layer_name}] {len(names)} polygons rasterised")

        npz_bytes = masks_to_npz_bytes(masks, names)
        r2.put_object(Bucket=R2_BUCKET, Key=npz_key, Body=npz_bytes,
                      ContentType="application/octet-stream")
        r2.put_object(Bucket=R2_BUCKET, Key=names_key,
                      Body=json.dumps(names).encode(),
                      ContentType="application/json; charset=utf-8")
        r2.put_object(Bucket=R2_BUCKET, Key=fp_key,
                      Body=json.dumps(current_fp).encode(),
                      ContentType="application/json; charset=utf-8")
        print(f"  [ukv_masks/{layer_name}] cached to R2")
        all_masks[layer_name] = masks

    return all_masks


# ── Valid time label ──────────────────────────────────────────────────────────────
def valid_label_str(valid_ts):
    local, tz = _uk_local(parse_run_dt(valid_ts).replace(tzinfo=timezone.utc))
    return f"{local.day} " + local.strftime("%b %Y %H:%M") + f" {tz}"


def cleanup_old_ukv_runs(r2, active_run_ts):
    """Delete R2 objects for UKV runs that are no longer in the active metadata run list."""
    print("Cleaning up old UKV runs from R2...")
    
    r2_runs = []
    try:
        paginator = r2.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix="ukv/", Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                prefix = cp["Prefix"]  # e.g. "ukv/20260515T0600Z/"
                run_ts = prefix.rstrip("/").split("/")[-1]
                if run_ts:
                    r2_runs.append(run_ts)
    except Exception as e:
        print(f"Error listing R2 folders under ukv/: {e}")
        return

    print(f"Found {len(r2_runs)} runs on R2 under ukv/")
    
    deleted_runs = 0
    # For each run on R2, if it's not active, delete all associated objects
    for run_ts in r2_runs:
        if run_ts in active_run_ts:
            continue
            
        print(f"  Purging inactive run {run_ts}...")
        deleted_runs += 1
        
        # Deleting all objects under associated prefixes
        prefixes_to_delete = [
            f"ukv/{run_ts}/",
            f"ukv_poly/{run_ts}/",
            f"ukv_gauge/{run_ts}/",
        ]
        for prefix in prefixes_to_delete:
            deleted_count = 0
            paginator_del = r2.get_paginator("list_objects_v2")
            for page in paginator_del.paginate(Bucket=R2_BUCKET, Prefix=prefix):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    r2.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": keys})
                    deleted_count += len(keys)
            if deleted_count > 0:
                print(f"    Deleted {deleted_count} objects under {prefix}")
                
        # Also delete the single ukv_area_ts file
        area_ts_key = f"ukv_area_ts/{run_ts}.json"
        try:
            r2.delete_object(Bucket=R2_BUCKET, Key=area_ts_key)
            print(f"    Deleted {area_ts_key}")
        except Exception:
            pass

    print(f"UKV cleanup complete. Purged {deleted_runs} runs.")


# ── Run-to-run comparison ─────────────────────────────────────────────────────────

# Diverging colour scale for catchment diff PNGs (dark blue → white → dark red)
_DIFF_COLORS = np.array([
    ( 26,  35, 126, 220),   # < −25 mm  (dark blue)
    ( 21, 101, 192, 220),   # −25..−10  (blue)
    (100, 181, 246, 200),   # −10..−3   (light blue)
    (250, 250, 250,  50),   # −3..+3    (near-transparent — no signal)
    (255, 204, 128, 200),   # +3..+10   (light orange)
    (229,  57,  53, 220),   # +10..+25  (red)
    (183,  28,  28, 220),   # > +25 mm  (dark red)
], dtype=np.uint8)
_DIFF_BOUNDS = np.array([-25.0, -10.0, -3.0, 3.0, 10.0, 25.0], dtype=np.float32)

_COMP_FLOOR_CATCHMENT = 2.0   # minimum absolute delta to flag at catchment scale
_COMP_FLOOR_GRID      = 5.0   # minimum absolute delta to flag at 20km grid scale
_COMP_THRESHOLDS      = (10.0, 25.0, 50.0, 100.0)  # flood significance thresholds (mm/24h)
_COMP_CHANGE_FRAC     = 0.20  # also flag if 20% relative change


def _comp_is_significant(delta, prior, floor):
    """Return (is_significant, reason) where reason ∈ {'threshold','relative',None}."""
    if abs(delta) < floor:
        return False, None
    lo = min(prior, prior + delta)
    hi = max(prior, prior + delta)
    for t in _COMP_THRESHOLDS:
        if lo < t <= hi:
            return True, "threshold"
    if prior > 1.0 and abs(delta) / prior > _COMP_CHANGE_FRAC:
        return True, "relative"
    return False, None


def _find_matching_step(run, target_h):
    """Return step in run whose offset_hours is closest to target_h (within ±2h)."""
    best, best_diff = None, float("inf")
    for step in run.get("steps", []):
        oh = step.get("offset_hours")
        if oh is None:
            continue
        d = abs(oh - target_h)
        if d < best_diff:
            best_diff, best = d, step
    return best if best_diff <= 2 else None


def _load_poly_layer(r2, run_ts, step, accum_key, layer):
    """Load poly JSON for a step, return {area_name: mm} for given layer/accum_key."""
    if step is None:
        return {}
    offset_str = step.get("offset", "")
    if not offset_str:
        return {}
    poly = json_from_r2(r2, f"ukv_poly/{run_ts}/{offset_str}.json")
    return ((poly or {}).get(layer) or {}).get(accum_key) or {}


def _render_diff_png(ukv_masks, layer_name, area_deltas):
    """Render a diverging choropleth diff PNG, auto-cropped to the data extent.

    The full output grid is mostly empty ocean, so we crop to the bounding box of
    the rendered polygons (with padding) before resizing — this makes catchments
    fill the frame instead of being a tiny cluster lost in the domain.
    """
    layer_masks = (ukv_masks or {}).get(layer_name)
    if not layer_masks:
        return None
    canvas = np.zeros((HEIGHT, WIDTH, 4), dtype=np.uint8)
    for area_name, delta in area_deltas.items():
        mask = layer_masks.get(area_name)
        if mask is None:
            continue
        rows, cols = mask
        if len(rows) == 0:
            continue
        idx = int(np.searchsorted(_DIFF_BOUNDS, float(delta)))
        idx = max(0, min(idx, len(_DIFF_COLORS) - 1))
        canvas[rows, cols] = _DIFF_COLORS[idx]

    # Auto-crop to non-transparent extent (all rendered polygons have alpha > 0).
    ys, xs = np.where(canvas[:, :, 3] > 0)
    if len(ys) == 0:
        return None
    pad = 60
    r0, r1 = max(0, int(ys.min()) - pad), min(HEIGHT, int(ys.max()) + pad)
    c0, c1 = max(0, int(xs.min()) - pad), min(WIDTH,  int(xs.max()) + pad)
    canvas = canvas[r0:r1, c0:c1]

    img = Image.fromarray(canvas, "RGBA")
    thumb_w = 600
    thumb_h = max(1, round(canvas.shape[0] * thumb_w / canvas.shape[1]))
    return img.resize((thumb_w, thumb_h), Image.LANCZOS)


def _label_to_ddhhmm(label):
    """Convert run_label_str output like '25 Jun 2026 06:00 GMT'/'2 Jul 2026 00:00 BST'
    to 'Mon 25/0600 GMT'/'Wed 2/0000 BST'. The input is already the correctly
    localized UK wall-clock time (from run_label_str/valid_label_str) — this
    only reformats it, it must NOT re-interpret it as UTC or overwrite its
    BST/GMT suffix with a hardcoded one."""
    try:
        s = label.strip()
        for tz in ("BST", "GMT"):
            if s.endswith(tz):
                dt = datetime.strptime(s[:-len(tz)].strip(), "%d %b %Y %H:%M")
                return dt.strftime("%a") + f" {dt.day}" + dt.strftime("/%H%M") + f" {tz}"
        return label
    except (ValueError, AttributeError):
        return label


def _generate_trends_insights(comparisons):
    """Produce plain-English insight statements from cross-run comparison data."""
    if not comparisons:
        return ["No run-to-run comparison data available."]

    # The immediate predecessor comparison (first entry) drives the headline.
    primary = comparisons[0]
    insights = []

    # ── 1. Headline: net day-1 direction vs the immediate prior run ─────────
    p_catch = primary.get("windows", {}).get("day1", {}).get("catchments", {})
    sig_up   = [(n, d["delta_mm"]) for n, d in p_catch.items() if d.get("significant") and d["delta_mm"] > 0]
    sig_down = [(n, d["delta_mm"]) for n, d in p_catch.items() if d.get("significant") and d["delta_mm"] < 0]
    lbl = f"{_label_to_ddhhmm(primary['run_b_label'])} → {_label_to_ddhhmm(primary['run_a_label'])}"

    if not sig_up and not sig_down:
        insights.append(f"Day-1 picture broadly unchanged vs the prior run ({lbl}) — no catchment crossed a significance rule.")
    elif sig_up and not sig_down:
        insights.append(f"Latest run is wetter for day 1: {len(sig_up)} catchment(s) up significantly, none down ({lbl}).")
    elif sig_down and not sig_up:
        insights.append(f"Latest run is drier for day 1: {len(sig_down)} catchment(s) down significantly, none up ({lbl}).")
    else:
        insights.append(f"Mixed day-1 signal vs prior run: {len(sig_up)} catchment(s) wetter, {len(sig_down)} drier ({lbl}).")

    # ── 2. Largest individual shift (across all comparisons, day 1) ─────────
    largest_abs, largest_info = 0.0, None
    for comp in comparisons:
        for name, d in comp.get("windows", {}).get("day1", {}).get("catchments", {}).items():
            if d.get("significant") and abs(d["delta_mm"]) > largest_abs:
                largest_abs  = abs(d["delta_mm"])
                largest_info = (name, d["delta_mm"], d.get("run_a_mm"), d.get("run_b_mm"),
                                comp["run_a_label"], comp["run_b_label"])
    if largest_info:
        name, delta, a_mm, b_mm, lbl_a, lbl_b = largest_info
        direction = "wetter" if delta > 0 else "drier"
        totals = ""
        if a_mm is not None and b_mm is not None:
            totals = f" ({b_mm:.0f} → {a_mm:.0f}mm)"
        insights.append(
            f"Largest shift: {name} {direction} by {abs(delta):.0f}mm/24h{totals} "
            f"from {_label_to_ddhhmm(lbl_b)} to {_label_to_ddhhmm(lbl_a)}."
        )

    # ── 3. Region-level spatial summary (where is the change concentrated) ──
    reg_up, reg_down = {}, {}
    for name, d in primary.get("windows", {}).get("day1", {}).get("regions", {}).items():
        if not d.get("significant"):
            continue
        (reg_up if d["delta_mm"] > 0 else reg_down).setdefault(name, d["delta_mm"])
    if reg_up:
        top = sorted(reg_up.items(), key=lambda kv: -kv[1])[:3]
        insights.append("Wetter signal focused over: " + ", ".join(f"{n} (+{v:.0f}mm)" for n, v in top) + ".")
    if reg_down:
        top = sorted(reg_down.items(), key=lambda kv: kv[1])[:3]
        insights.append("Drier signal focused over: " + ", ".join(f"{n} ({v:.0f}mm)" for n, v in top) + ".")

    # ── 4. Consistency across the run sequence ──────────────────────────────
    if len(comparisons) >= 2:
        dir_per_run = []
        for comp in comparisons:
            c = comp.get("windows", {}).get("day1", {}).get("catchments", {})
            up   = sum(1 for d in c.values() if d.get("significant") and d["delta_mm"] > 0)
            down = sum(1 for d in c.values() if d.get("significant") and d["delta_mm"] < 0)
            dir_per_run.append(1 if up > down else (-1 if down > up else 0))
        nonzero = [d for d in dir_per_run if d != 0]
        if nonzero and all(d == nonzero[0] for d in nonzero):
            word = "wetter" if nonzero[0] > 0 else "drier"
            insights.append(f"Trend is consistent across the last {len(comparisons)} runs (all leaning {word}).")
        elif nonzero:
            insights.append("Run-to-run signal is unsettled — direction differs between recent runs.")

    # ── 5. Day-2 outlook ────────────────────────────────────────────────────
    day2_up = sum(1 for n, d in primary.get("windows", {}).get("day2", {}).get("catchments", {}).items()
                  if d.get("significant") and d["delta_mm"] > 0)
    day2_down = sum(1 for n, d in primary.get("windows", {}).get("day2", {}).get("catchments", {}).items()
                    if d.get("significant") and d["delta_mm"] < 0)
    if day2_up or day2_down:
        if day2_up and not day2_down:
            insights.append(f"Day 2 also trending wetter ({day2_up} catchment(s)).")
        elif day2_down and not day2_up:
            insights.append(f"Day 2 also trending drier ({day2_down} catchment(s)).")
        else:
            insights.append(f"Day 2 mixed: {day2_up} wetter, {day2_down} drier.")

    if not insights:
        insights.append("No significant run-to-run changes in this update.")

    return insights


def compute_run_trends(r2, meta, ukv_masks):
    """Compute run-to-run comparison stats, render diff PNG, write ukv_trends.json."""
    runs = meta.get("runs", [])
    if len(runs) < 2:
        print("  Trends: need ≥2 runs — skipping.")
        return

    latest  = runs[0]
    lat_ts  = latest["run_ts"]
    lat_dt  = parse_run_dt(lat_ts)
    lat_lbl = latest.get("run_label", lat_ts)

    # Select prior comparison runs up to 24h lag (deduped)
    seen, cand = {lat_ts}, []
    for r in runs[1:]:
        rt = r["run_ts"]
        if rt not in seen:
            seen.add(rt)
            cand.append(r)
        if len(cand) >= 10:
            break

    if not cand:
        print("  Trends: no comparison runs found.")
        return

    WINDOWS = [
        ("12h",  "accum_12h", 12),
        ("day1", "accum_24h", 24),
        ("day2", "accum_24h", 48),   # rolling 24h ending at T+48 = T+24→T+48
    ]
    # Floors per layer: catchments/regions use the catchment floor, grid uses its own.
    LAYERS = [
        ("catchments", _COMP_FLOOR_CATCHMENT),
        ("regions",    _COMP_FLOOR_CATCHMENT),
        ("grid",       _COMP_FLOOR_GRID),
    ]

    comparisons     = []
    first_pair_diff = None

    for comp_run in cand:
        comp_ts  = comp_run["run_ts"]
        comp_dt  = parse_run_dt(comp_ts)
        comp_lbl = comp_run.get("run_label", comp_ts)
        lag_h    = (lat_dt - comp_dt).total_seconds() / 3600

        entry = {
            "run_a": lat_ts, "run_a_label": lat_lbl,
            "run_b": comp_ts, "run_b_label": comp_lbl,
            "label": f"-{lag_h:.0f}h run",
            "windows": {},
        }

        for win_key, accum_key, off_h_a in WINDOWS:
            target_off_h_b = off_h_a + lag_h
            step_a = _find_matching_step(latest,   off_h_a)
            step_b = _find_matching_step(comp_run, target_off_h_b)

            win_data = {}
            for layer_name, floor in LAYERS:
                vals_a = _load_poly_layer(r2, lat_ts,  step_a, accum_key, layer_name)
                vals_b = _load_poly_layer(r2, comp_ts, step_b, accum_key, layer_name)

                layer_deltas = {}
                for name, va in vals_a.items():
                    vb = vals_b.get(name)
                    if va is None or vb is None:
                        continue
                    delta = round(float(va) - float(vb), 2)
                    sig, reason = _comp_is_significant(delta, float(vb), floor)
                    layer_deltas[name] = {
                        "delta_mm":    delta,
                        "run_a_mm":    round(float(va), 2),
                        "run_b_mm":    round(float(vb), 2),
                        "significant": sig,
                        "reason":      reason,
                    }
                win_data[layer_name] = layer_deltas

            entry["windows"][win_key] = win_data

            # Day-1 specials: diff PNG + raw UKV raster URLs for the actual totals
            if win_key == "day1":
                if step_a:
                    entry["raw_image_a_url"] = (
                        f"{R2_PUBLIC_URL}/ukv/{lat_ts}/{step_a['offset']}_accum_24h_met.png"
                    )
                if step_b:
                    entry["raw_image_b_url"] = (
                        f"{R2_PUBLIC_URL}/ukv/{comp_ts}/{step_b['offset']}_accum_24h_met.png"
                    )
                if first_pair_diff is None and comp_run is cand[0]:
                    catchment_deltas = {
                        k: v["delta_mm"] for k, v in win_data.get("catchments", {}).items()
                    }
                    if catchment_deltas:
                        first_pair_diff = _render_diff_png(ukv_masks, "catchments", catchment_deltas)

        if first_pair_diff is not None and comp_run is cand[0]:
            diff_key = f"ukv_trends/{lat_ts}_vs_{comp_ts}_day1_diff.png"
            png_to_r2(r2, diff_key, first_pair_diff)
            entry["diff_image_url"] = f"{R2_PUBLIC_URL}/{diff_key}"
            first_pair_diff = None   # mark as uploaded

        comparisons.append(entry)

    insights = _generate_trends_insights(comparisons)

    trends = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00Z"),
        "latest_run":   lat_ts,
        "latest_label": lat_lbl,
        "comparisons":  comparisons,
        "insights":     insights,
        "image_bounds": [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]],
        "significance_rules": {
            "catchment_floor_mm":  _COMP_FLOOR_CATCHMENT,
            "grid_floor_mm":       _COMP_FLOOR_GRID,
            "thresholds_mm":       list(_COMP_THRESHOLDS),
            "relative_change_pct": round(_COMP_CHANGE_FRAC * 100),
        },
    }

    with open("ukv_trends.json", "w") as f:
        json.dump(trends, f, indent=2)
    json_to_r2(r2, "ukv_trends.json", trends)
    print(f"  Trends: {len(comparisons)} comparison(s). {insights[0][:80]}")


def backfill_missing_splats(s3, r2, meta, mapping=None):
    """Backfill missing splats for the top active 5 runs in metadata using parallel multi-threaded execution."""
    runs = meta.get("runs", [])
    target_runs = runs[:5]  # check top 5 active runs
    any_updated = False
    for run in target_runs:
        run_ts = run.get("run_ts")
        steps = run.get("steps", [])
        if not run_ts or not steps:
            continue
        missing = any("splats" not in s or not s["splats"] for s in steps)
        if not missing:
            continue
        print(f"  Backfilling missing splats for active run {run_ts} (parallel acceleration)...")
        s3_steps = discover_steps(s3, run_ts)
        if not s3_steps:
            continue

        if mapping is None:
            first_hours, first_offset, first_valid = s3_steps[0]
            nc_bytes = download_nc(s3, run_ts, first_valid, first_offset, "rainfall_rate")
            if nc_bytes is None:
                continue
            with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                tmp.write(nc_bytes)
                sample_path = tmp.name
            try:
                mapping = build_mapping(sample_path)
            finally:
                os.unlink(sample_path)

        # Download and extract array slots in parallel with 8 threads
        step_slots = {}
        def _fetch_one_step(idx, step_tuple):
            hours, offset, valid_ts = step_tuple
            arr_rate, arr_accum_1h = load_arr_both(s3, run_ts, valid_ts, offset, mapping)
            if arr_accum_1h is not None:
                return offset, {"arr": arr_accum_1h, "hours": 1}
            elif arr_rate is not None:
                prev_h = s3_steps[idx - 1][0] if idx > 0 else 0
                step_h = hours - prev_h
                return offset, {"arr": arr_rate * step_h, "hours": step_h}
            return offset, None

        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = [ex.submit(_fetch_one_step, idx, s_tuple) for idx, s_tuple in enumerate(s3_steps)]
            for fut in as_completed(futs):
                off, slot = fut.result()
                if slot:
                    step_slots[off] = slot

        # Build accum stack and prepare upload tasks in order
        accum_stack = []
        upload_tasks = []
        for idx, (hours, offset, valid_ts) in enumerate(s3_steps):
            meta_step = next((s for s in steps if s.get("offset") == offset), None)
            if not meta_step or offset not in step_slots:
                continue
            accum_stack.append(step_slots[offset])

            total_stk = sum(s["hours"] for s in accum_stack)
            while total_stk > 48 and accum_stack:
                total_stk -= accum_stack.pop(0)["hours"]

            accum_renders = []
            for n in ACCUM_PERIODS:
                if total_stk < n:
                    continue
                covered, arrs = 0, []
                for slot in reversed(accum_stack):
                    if covered >= n:
                        break
                    if slot["hours"] <= n - covered:
                        arrs.append(slot["arr"])
                        covered += slot["hours"]
                    else:
                        break
                if covered < n:
                    continue
                accum_renders.append((n, np.sum(arrs, axis=0).astype(np.float32)))

            splat_results = {}
            for n, arr_n in accum_renders:
                dur_key = f"{n}h"
                if dur_key in SPLAT_THRESHOLDS:
                    for th in SPLAT_THRESHOLDS[dur_key]:
                        th_str = f"{int(th)}mm" if th == int(th) else f"{th}mm"
                        skey = f"ukv/{run_ts}/{offset}_splat_{dur_key}_{th_str}.png"
                        upload_tasks.append((arr_n, th, skey))
                        splat_results[f"{dur_key}_{th_str}"] = f"{R2_PUBLIC_URL}/{skey}"

            if splat_results:
                meta_step["splats"] = splat_results
                any_updated = True

        # Upload generated splat PNGs to R2 in parallel
        if upload_tasks:
            def _upload_splat(task):
                arr_n, th, skey = task
                splat_img = render_splat_png(arr_n, th)
                png_to_r2(_r2_tl(), skey, splat_img)

            with ThreadPoolExecutor(max_workers=8) as ex:
                list(ex.map(_upload_splat, upload_tasks))

        print(f"    {run_ts}: successfully rendered & uploaded splats for {len(step_slots)} steps.")

    return any_updated


# ── Main ─────────────────────────────────────────────────────────────────────────
def main():
    if not USE_R2:
        print("R2 credentials not set — aborting.")
        sys.exit(1)

    s3 = get_s3()
    r2 = get_r2()

    existing_meta = json_from_r2(r2, "ukv_meta.json", {"runs": []})
    existing_runs = existing_meta.get("runs", [])

    print("Checking active runs for missing splat data...")
    if backfill_missing_splats(s3, r2, existing_meta, None):
        with open("ukv_meta.json", "w") as f:
            json.dump(existing_meta, f, indent=2)
        json_to_r2(r2, "ukv_meta.json", existing_meta)
        print("  Backfill complete and updated metadata uploaded.")
    else:
        print("  Active runs already up to date with splats.")
    print("Finding latest complete UKV run...")
    run_ts = find_latest_run(s3)
    if not run_ts:
        print("No complete run found on S3 yet.")
        sys.exit(0)
    print(f"  Latest complete: {run_ts}")

    force = os.environ.get("FORCE_RERUN", "").lower() in ("1", "true", "yes")
    try:
        keep_runs = int(os.environ.get("KEEP_RUNS", "30"))
    except ValueError:
        keep_runs = 30
    if not force and existing_runs and existing_runs[0].get("run_ts") == run_ts:
        print(f"  {run_ts} already processed — done.")
        sys.exit(0)

    print("Discovering forecast steps...")
    steps = discover_steps(s3, run_ts)
    if not steps:
        print("  No whole-hour steps found.")
        sys.exit(1)
    print(f"  {len(steps)} steps: T+{steps[0][0]}h → T+{steps[-1][0]}h")

    print("Building coordinate mapping from T+1h file...")
    first_hours, first_offset, first_valid = steps[0]
    nc_bytes = download_nc(s3, run_ts, first_valid, first_offset, "rainfall_rate")
    if nc_bytes is None:
        print("  Cannot build mapping — aborting.")
        sys.exit(1)
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp.write(nc_bytes)
        sample_path = tmp.name
    try:
        mapping = build_mapping(sample_path)
    finally:
        os.unlink(sample_path)
    print("  Mapping ready.")

    rlabel = run_label_str(run_ts)
    print(f"  Run: {rlabel}")

    print("Loading polygon masks...")
    ukv_masks = load_or_build_ukv_masks(r2)
    print(f"  Masks ready for {len(ukv_masks)} boundary layer(s)")

    # Build station pixel positions on the UKV output Mercator grid
    station_pixels_ukv = {}
    if os.path.exists("rain/stations.json"):
        with open("rain/stations.json") as f:
            _st = json.load(f).get("stations", {})
        ll_to_merc_st = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        _mx0, _my0 = ll_to_merc_st.transform(LON_MIN, LAT_MIN)
        _mx1, _my1 = ll_to_merc_st.transform(LON_MAX, LAT_MAX)
        for sid, s in _st.items():
            mx, my = ll_to_merc_st.transform(s["lon"], s["lat"])
            col = round((mx - _mx0) / (_mx1 - _mx0) * (WIDTH  - 1))
            row = round((_my1 - my) / (_my1 - _my0) * (HEIGHT - 1))
            if 0 <= row < HEIGHT and 0 <= col < WIDTH:
                station_pixels_ukv[sid] = (row, col)
        print(f"  {len(station_pixels_ukv)} gauge stations mapped to UKV grid")

    area_ts_steps = []
    step_entries = []
    # Stack of {'arr': ndarray, 'hours': int, 'estimated': bool}.
    # Real 1h accumulation files are used for T+1..T+54; beyond that the Met
    # Office publishes only 3-hourly rate files, so we approximate accumulation
    # as rate_mm_hr × step_interval_hours and flag those slots as estimated.
    accum_stack = []

    for i, (hours, offset, valid_ts) in enumerate(steps):
        vlabel = valid_label_str(valid_ts)
        print(f"  [{i+1}/{len(steps)}] {offset}  {vlabel}", flush=True)

        entry = {"offset": offset, "offset_hours": hours, "valid_label": vlabel}

        # Download rate + accum in parallel, then upload rate PNGs in parallel.
        arr, arr_1h = load_arr_both(s3, run_ts, valid_ts, offset, mapping)

        urls = upload_schemes(arr, RATE_SCHEMES, run_ts, offset, "rate")
        if urls:
            entry["rainfall_rate"] = urls

        # Accumulation: real 1h file if available, else approximate from rate.
        if arr_1h is not None:
            accum_stack.append({"arr": arr_1h, "hours": 1, "estimated": False})
        elif arr is not None:
            step_h = hours - steps[i - 1][0] if i > 0 else hours
            print(f"      [estimated] rate×{step_h}h as accum proxy")
            accum_stack.append({"arr": arr * step_h, "hours": step_h, "estimated": True})

        # Trim stack to last 48h
        total_stk = sum(s["hours"] for s in accum_stack)
        while total_stk > 48 and accum_stack:
            total_stk -= accum_stack.pop(0)["hours"]

        if accum_stack:
            total_stk   = sum(s["hours"] for s in accum_stack)
            arrays_for_poly = {}
            any_estimated   = False

            # Collect all (arr_n, n, is_est) first, then upload all accum PNGs in parallel.
            accum_renders = []
            for n in ACCUM_PERIODS:
                if total_stk < n:
                    continue
                # Walk backwards through the stack, accumulating whole slots until
                # n hours are covered. If a slot is too large for the remaining
                # budget, stop — this prevents bridging across a temporal gap
                # (e.g. a 3h slot can't contribute to a 1h accumulation).
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
                urls = upload_schemes(arr_n, ACCUM_SCHEMES, run_ts, offset, f"accum_{n}h")
                splats = {}
                dur_key = f"{n}h"
                if dur_key in SPLAT_THRESHOLDS:
                    for th in SPLAT_THRESHOLDS[dur_key]:
                        th_str = f"{int(th)}mm" if th == int(th) else f"{th}mm"
                        splat_img = render_splat_png(arr_n, th)
                        skey = f"ukv/{run_ts}/{offset}_splat_{dur_key}_{th_str}.png"
                        png_to_r2(_r2_tl(), skey, splat_img)
                        splats[f"{dur_key}_{th_str}"] = f"{R2_PUBLIC_URL}/{skey}"
                return n, arr_n, urls, is_est, splats

            if accum_renders:
                with ThreadPoolExecutor(max_workers=min(len(accum_renders), 6)) as ex:
                    futs = [ex.submit(_upload_accum, n, a, e) for n, a, e in accum_renders]
                    for fut in as_completed(futs):
                        n, arr_n, urls, is_est, splats = fut.result()
                        entry[f"accum_{n}h"] = urls
                        arrays_for_poly[f"accum_{n}h"] = arr_n
                        if splats:
                            if "splats" not in entry:
                                entry["splats"] = {}
                            entry["splats"].update(splats)
                        if is_est:
                            any_estimated = True

            if any_estimated:
                entry["accum_estimated"] = True

            if arrays_for_poly and ukv_masks:
                poly_data = compute_poly_averages(arrays_for_poly, ukv_masks)

                def _upload_poly_gauge():
                    _r2_tl().put_object(
                        Bucket=R2_BUCKET,
                        Key=f"ukv_poly/{run_ts}/{offset}.json",
                        Body=json.dumps(poly_data).encode(),
                        ContentType="application/json; charset=utf-8",
                    )

                gauge_body = None
                if station_pixels_ukv:
                    gauge_data = {}
                    for sid, (row, col) in station_pixels_ukv.items():
                        vals = {k: round(float(a[row, col]), 2)
                                for k, a in arrays_for_poly.items()
                                if float(a[row, col]) >= 0}
                        if vals:
                            gauge_data[sid] = vals
                    if gauge_data:
                        gauge_body = json.dumps(gauge_data, separators=(",", ":")).encode()

                def _upload_gauge():
                    if gauge_body:
                        _r2_tl().put_object(
                            Bucket=R2_BUCKET,
                            Key=f"ukv_gauge/{run_ts}/{offset}.json",
                            Body=gauge_body,
                            ContentType="application/json; charset=utf-8",
                        )

                with ThreadPoolExecutor(max_workers=2) as ex:
                    list(ex.map(lambda fn: fn(), [_upload_poly_gauge, _upload_gauge]))

                ts_entry = {'hours': hours}
                for layer_name in ukv_masks:
                    ts_entry[layer_name] = poly_data.get(layer_name, {}).get('accum_1h', {})
                area_ts_steps.append(ts_entry)

        step_entries.append(entry)

    if area_ts_steps:
        area_ts = {'step_hours': [s['hours'] for s in area_ts_steps]}
        for layer_name in ukv_masks:
            area_ts[layer_name] = {}
            for area_name in {a for s in area_ts_steps for a in s.get(layer_name, {})}:
                area_ts[layer_name][area_name] = [
                    s.get(layer_name, {}).get(area_name) for s in area_ts_steps
                ]
        r2.put_object(
            Bucket=R2_BUCKET,
            Key=f'ukv_area_ts/{run_ts}.json',
            Body=json.dumps(area_ts, separators=(',', ':')).encode(),
            ContentType='application/json; charset=utf-8',
        )

    new_run = {
        "run_ts":         run_ts,
        "run_label":      rlabel,
        "forecast_hours": steps[-1][0],
        "steps":          step_entries,
    }

    # Keep at most KEEP_RUNS older runs (retained up to 72h / 3 days).
    def _retention_hours(rt):
        return 72

    now  = datetime.utcnow()
    seen = {run_ts}
    older = []
    for r in existing_runs:
        rt = r.get("run_ts")
        if not rt or rt in seen:
            continue
        try:
            if parse_run_dt(rt) < now - timedelta(hours=_retention_hours(rt)):
                continue
        except Exception:
            continue
        seen.add(rt)
        older.append(r)
    older = older[:max(0, keep_runs)]

    meta = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00.000Z"),
        "runs": [new_run] + older,
    }

    print("Checking active runs for missing splats...")
    backfill_missing_splats(s3, r2, meta, mapping)

    with open("ukv_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    json_to_r2(r2, "ukv_meta.json", meta)
    print(f"Done: {len(meta['runs'])} run(s) in meta, {len(step_entries)} steps this run.")

    print("Computing run-to-run trends...")
    compute_run_trends(r2, meta, ukv_masks)

    # Run paginated cleanup of old inactive UKV runs from Cloudflare R2
    active_run_ts = {r["run_ts"] for r in meta["runs"]}
    cleanup_old_ukv_runs(r2, active_run_ts)


if __name__ == "__main__":
    main()
