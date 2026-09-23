"""
Generate synthetic segments in the exact comma2k19 on-disk layout, with
safety events injected at KNOWN times. Used for tests (ground truth) and for
demoing the dashboard before downloading the real 100 GB dataset.

This is NOT real driving data - report metrics from comma2k19, not from this.

Usage:
    python tools/make_synthetic.py --out data/synthetic --routes 6 --segments 10
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

DONGLES = ["b0c9d2329ad1606b", "99c94dc769b5d96e"]  # RAV4, Civic


def _save(path: Path, arr: np.ndarray) -> None:
    """comma2k19 stores .npy data in files with NO extension."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        np.save(f, arr)


def make_segment(seg_dir: Path, t0: float, rng: np.random.Generator,
                 brake_at: float | None, cutin_at: float | None) -> dict:
    """60 s segment. Returns ground truth for the injected events."""
    hz = 100
    t = t0 + np.arange(0, 60, 1 / hz)
    rel = t - t0

    # cruise ~25 m/s with gentle drift
    accel = 0.15 * np.sin(rel / 7.0) + rng.normal(0, 0.05, len(t))
    truth = {}
    if brake_at is not None:
        # 1.5 s at about -5 m/s^2 (hard brake), then recover
        m = (rel >= brake_at) & (rel < brake_at + 1.5)
        accel[m] = -5.0 + rng.normal(0, 0.1, m.sum())
        rec = (rel >= brake_at + 3) & (rel < brake_at + 6)
        accel[rec] = 3.0  # hard acceleration back to speed
        truth["hard_brake_t"] = t0 + brake_at
        truth["hard_accel_t"] = t0 + brake_at + 3
    speed = np.clip(25 + np.cumsum(accel) / hz, 0, None)
    # quantise like a real CAN signal
    speed_can = np.round(speed * 36) / 36

    _save(seg_dir / "processed_log/CAN/speed/t", t)
    _save(seg_dir / "processed_log/CAN/speed/value", speed_can.reshape(-1, 1))
    _save(seg_dir / "processed_log/CAN/steering_angle/t", t)
    _save(seg_dir / "processed_log/CAN/steering_angle/value",
          np.round(2 * np.sin(rel / 11.0) + rng.normal(0, 0.2, len(t)), 1))

    # radar at 20 Hz: one lead car, normally 40 m ahead and matching speed
    tr = t0 + np.arange(0, 60, 0.05)
    rr = tr - t0
    dist = np.full(len(tr), 40.0) + rng.normal(0, 0.3, len(tr))
    vrel = rng.normal(0, 0.1, len(tr))
    if cutin_at is not None:
        # a car cuts in at 25 m, closing at 10 m/s for 1.5 s -> TTC dips < 3 s
        m = (rr >= cutin_at) & (rr < cutin_at + 1.5)
        k = rr[m] - cutin_at
        dist[m] = 25 - 10 * k
        vrel[m] = -10.0
        truth["low_ttc_t"] = t0 + cutin_at
    radar = np.column_stack([
        dist, rng.normal(0, 0.3, len(tr)), vrel,
        np.full(len(tr), np.nan), np.full(len(tr), np.nan),
        np.full(len(tr), 530.0), np.zeros(len(tr)),
    ])
    _save(seg_dir / "processed_log/CAN/radar/t", tr)
    _save(seg_dir / "processed_log/CAN/radar/value", radar)

    tg = t0 + np.arange(0, 60, 2.0)
    gnss = np.column_stack([
        37.7 + 0.001 * np.arange(len(tg)), np.full(len(tg), -122.47),
        np.interp(tg, t, speed), 1.5e12 + 1000 * (tg - t0),
        np.full(len(tg), 40.0), np.full(len(tg), 180.0),
    ])
    _save(seg_dir / "processed_log/GNSS/live_gnss_qcom/t", tg)
    _save(seg_dir / "processed_log/GNSS/live_gnss_qcom/value", gnss)
    return truth


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--routes", type=int, default=6)
    ap.add_argument("--segments", type=int, default=10)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    rng = np.random.default_rng(args.seed)

    truth = {}
    for r in range(args.routes):
        dongle = DONGLES[r % 2]
        route = f"{dongle}|2018-08-{1 + r // 2:02d}--08-{10 + r:02d}-00"
        for s in range(args.segments):
            brake_at = float(rng.uniform(10, 45)) if rng.random() < 0.3 else None
            cutin_at = float(rng.uniform(10, 45)) if rng.random() < 0.2 else None
            # keep the two apart unless we deliberately want a compound scenario
            if brake_at and cutin_at and rng.random() < 0.5:
                cutin_at = brake_at - 0.5  # compound: cut-in then hard brake
            seg_dir = args.out / "Chunk_synth" / route / str(s)
            gt = make_segment(seg_dir, t0=1000.0 + 60 * s, rng=rng,
                              brake_at=brake_at, cutin_at=cutin_at)
            truth[f"{route}/{s}"] = gt

    (args.out / "ground_truth.json").write_text(json.dumps(truth, indent=1))
    n_b = sum("hard_brake_t" in v for v in truth.values())
    n_t = sum("low_ttc_t" in v for v in truth.values())
    print(f"wrote {len(truth)} segments; injected {n_b} hard brakes, {n_t} low-TTC cut-ins")


if __name__ == "__main__":
    main()
