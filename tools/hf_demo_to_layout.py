"""
Unpack comma2k19 demo Parquet files (Hugging Face mirror) into the standard
comma2k19 folder layout, so pipeline/ingest.py can read them unchanged.
Keys in the `log` dict are file paths with '/' replaced by '__', e.g.
    processed_log__CAN__speed__t -> processed_log/CAN/speed/t
Usage: python tools/hf_demo_to_layout.py --parquet data/hf --out data/raw
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

WANTED = {f"{sig}__{f}" for sig in (
    "processed_log__CAN__speed", "processed_log__CAN__steering_angle", "processed_log__CAN__radar",
    "processed_log__IMU__accelerometer", "processed_log__GNSS__live_gnss_qcom",
) for f in ("t", "value")}


def _to_ndarray(x) -> np.ndarray:
    a = np.asarray(x)
    if a.dtype == object:
        a = np.array([np.asarray(e, dtype=np.float64) for e in a])
    return a.astype(np.float64)


def _save(path: Path, arr) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        np.save(f, _to_ndarray(arr))


def unpack(parquet_dir: Path, out_root: Path) -> int:
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        raise SystemExit(f"No .parquet files in {parquet_dir}")
    n = 0
    for f in files:
        df = pd.read_parquet(f, columns=["segment_id", "log"])
        for _, row in df.iterrows():
            log = row["log"]
            if not isinstance(log, dict):
                raise SystemExit(f"Unexpected `log` type {type(log)} in {f.name}; expected dict")
            keys = [k for k in log if k in WANTED and log[k] is not None]
            if not any(k.startswith("processed_log__CAN__speed") for k in keys):
                print(f"WARNING {row['segment_id']}: no CAN speed. keys sample: {list(log)[:15]}")
            seg_dir = out_root / "HF_demo" / row["segment_id"]
            for k in keys:
                _save(seg_dir / k.replace("__", "/"), log[k])
            n += 1
        print(f"{f.name}: {len(df)} segments")
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    n = unpack(args.parquet, args.out)
    print(f"unpacked {n} segments into {args.out}/HF_demo")


if __name__ == "__main__":
    main()
