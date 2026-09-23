"""
Bronze ingestion: comma2k19 numpy logs -> Parquet.

comma2k19 layout (per 1-minute segment):
    <chunk>/<dongle_id>|<route_start>/<segment_num>/processed_log/<SENSOR>/{t,value}
where `t` is a timestamp array (seconds since device boot) and `value` the readings.
Both are numpy .npy files saved without an extension.

Spark can't read .npy natively, so this step flattens each sensor stream into a
long-format Parquet table (one file per segment per table). Everything after this
is Spark SQL.

Usage:
    python pipeline/ingest.py --raw data/raw --out data/bronze --workers 8
"""
from __future__ import annotations

import argparse
import logging
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("ingest")

# From the comma2k19 README: chunks 1-2 are a RAV4, the rest a Civic.
VEHICLE_BY_DONGLE = {
    "b0c9d2329ad1606b": "toyota_rav4",
    "99c94dc769b5d96e": "honda_civic",
}


def _col(v: np.ndarray, i: int = 0) -> np.ndarray:
    return v.reshape(len(v), -1)[:, i]


# table name -> (relative sensor path, row builder)
SIGNALS = {
    "can_speed": (
        "CAN/speed",
        lambda t, v: {"t": t, "speed_mps": _col(v)},
    ),
    "can_steering": (
        "CAN/steering_angle",
        lambda t, v: {"t": t, "steer_deg": _col(v)},
    ),
    # radar columns: [fwd dist m, lateral dist m, relative speed m/s, nan, nan, address, new_track]
    # relative speed < 0 means the object is getting closer (verified against d(dist)/dt).
    "radar": (
        "CAN/radar",
        lambda t, v: {
            "t": t,
            "fwd_m": v[:, 0],
            "lat_m": v[:, 1],
            "rel_mps": v[:, 2],
            "track_id": np.nan_to_num(v[:, 5]).astype("int32"),
            "new_track": np.nan_to_num(v[:, 6]).astype(bool),
        },
    ),
    # live_gnss_qcom columns: [lat, lon, speed m/s, utc ms, altitude m, bearing deg]
    "gnss": (
        "GNSS/live_gnss_qcom",
        lambda t, v: {
            "t": t,
            "lat": v[:, 0],
            "lon": v[:, 1],
            "gnss_speed_mps": v[:, 2],
            "utc_ms": v[:, 3],
            "alt_m": v[:, 4],
            "bearing_deg": v[:, 5],
        },
    ),
}


def find_segments(raw_root: Path) -> list[Path]:
    """Every directory that contains a processed_log/ is one segment."""
    return sorted(p.parent for p in raw_root.rglob("processed_log") if p.is_dir())


def segment_meta(seg_dir: Path) -> dict:
    route = seg_dir.parent.name  # "<dongle_id>|<route_start>"
    dongle_id, _, route_start = route.partition("|")
    return {
        "segment_id": f"{route}/{seg_dir.name}",
        "route_id": route,
        "dongle_id": dongle_id,
        "vehicle": VEHICLE_BY_DONGLE.get(dongle_id, "unknown"),
        "segment_num": int(seg_dir.name),
        "route_start": route_start,
        # route_start looks like 2018-08-02--08-34-47
        "drive_date": route_start[:10] if len(route_start) >= 10 else None,
    }


def _safe_name(segment_id: str) -> str:
    return segment_id.replace("|", "_").replace("/", "__")


def ingest_segment(seg_dir: Path, out_root: Path, force: bool = False) -> dict:
    """Write one Parquet file per sensor table for a segment. Returns a small report."""
    meta = segment_meta(seg_dir)
    report = {"segment_id": meta["segment_id"], "rows": {}, "missing": [], "errors": []}
    fname = _safe_name(meta["segment_id"]) + ".parquet"

    for table, (rel, build) in SIGNALS.items():
        out = out_root / table / fname
        if out.exists() and not force:
            report["rows"][table] = -1  # already ingested (incremental re-runs)
            continue
        src = seg_dir / "processed_log" / rel
        if not (src / "t").exists() or not (src / "value").exists():
            report["missing"].append(table)
            continue
        try:
            t = np.load(src / "t")
            v = np.load(src / "value")
            if len(t) != len(v):
                raise ValueError(f"len(t)={len(t)} != len(value)={len(v)}")
            cols = build(t, v)
            cols = {"segment_id": np.full(len(t), meta["segment_id"]), **cols}
            out.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table(cols), out)
            report["rows"][table] = len(t)
        except Exception as e:  # keep going; bad segments show up in the DQ report
            report["errors"].append(f"{table}: {e}")
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=Path, required=True, help="comma2k19 root (chunks or Example_1)")
    ap.add_argument("--out", type=Path, required=True, help="bronze output dir")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--force", action="store_true", help="re-ingest segments already written")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    segs = find_segments(args.raw)
    if not segs:
        raise SystemExit(f"No processed_log/ directories found under {args.raw}")
    log.info("Found %d segments", len(segs))

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        reports = list(pool.map(ingest_segment, segs, [args.out] * len(segs), [args.force] * len(segs)))

    # Segment dimension table (always rewritten, it's tiny)
    metas = [segment_meta(s) for s in segs]
    (args.out / "segments").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(metas), args.out / "segments" / "segments.parquet")

    n_err = sum(bool(r["errors"]) for r in reports)
    n_missing = sum(bool(r["missing"]) for r in reports)
    for r in reports:
        if r["errors"]:
            log.warning("%s errors: %s", r["segment_id"], r["errors"])
    log.info("Ingested %d segments (%d with missing sensors, %d with errors)", len(segs), n_missing, n_err)


if __name__ == "__main__":
    main()
