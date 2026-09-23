import json
import subprocess
import sys

import pandas as pd
import pytest

from pipeline.spark_job import PARAMS, SQL_DIR, register_bronze, run_sql_file, run_sql_files
from tests.conftest import ROOT


TIMELINE_SCHEMA = "segment_id string, bucket long, t double, speed_mps double, accel_mps2 double, ttc_s double"


# ---------------------------------------------------------------------------
# Unit test: gaps-and-islands on a hand-built timeline
# ---------------------------------------------------------------------------
def test_gaps_and_islands_groups_consecutive_samples(spark):
    rows = []
    for b in range(0, 60):
        accel = 0.0
        if 10 <= b <= 14:   # 5 consecutive samples -> ONE event
            accel = -4.0
        if 30 <= b <= 31:   # only 2 samples -> below min_event_samples, dropped
            accel = -4.0
        if 40 <= b <= 43:   # separate island -> second event
            accel = -6.5
        rows.append(("seg", b, b / 10, 20.0, accel, None))
    pdf = pd.DataFrame(rows, columns=["segment_id", "bucket", "t", "speed_mps", "accel_mps2", "ttc_s"])
    spark.createDataFrame(pdf, TIMELINE_SCHEMA).createOrReplaceTempView("silver_timeline")

    run_sql_file(spark, SQL_DIR / "02_events.sql")
    ev = spark.table("events").where("event_type = 'hard_brake'").orderBy("start_t").toPandas()

    assert len(ev) == 2
    assert ev.start_t.tolist() == [1.0, 4.0]
    assert ev.duration_s.tolist() == [0.5, 0.4]
    assert ev.peak_value.tolist() == [-4.0, -6.5]
    assert ev.severity.iloc[1] == 1.0  # capped at 1


def test_low_speed_samples_are_ignored(spark):
    pdf = pd.DataFrame(
        [("seg", b, b / 10, 2.0, -5.0, None) for b in range(10)],  # 2 m/s, below min_speed
        columns=["segment_id", "bucket", "t", "speed_mps", "accel_mps2", "ttc_s"],
    )
    spark.createDataFrame(pdf, TIMELINE_SCHEMA).createOrReplaceTempView("silver_timeline")
    run_sql_file(spark, SQL_DIR / "02_events.sql")
    assert spark.table("events").count() == 0


# ---------------------------------------------------------------------------
# End-to-end: synthetic segments with known injected events
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def synthetic(tmp_path_factory, spark):
    d = tmp_path_factory.mktemp("syn")
    subprocess.run([sys.executable, str(ROOT / "tools/make_synthetic.py"),
                    "--out", str(d / "raw"), "--routes", "4", "--segments", "6", "--seed", "3"], check=True)
    subprocess.run([sys.executable, str(ROOT / "pipeline/ingest.py"),
                    "--raw", str(d / "raw"), "--out", str(d / "bronze"), "--workers", "2"], check=True)
    register_bronze(spark, d / "bronze")
    run_sql_files(spark)
    truth = json.loads((d / "raw/ground_truth.json").read_text())
    return spark, truth


@pytest.mark.parametrize("event_type,key", [
    ("hard_brake", "hard_brake_t"),
    ("hard_accel", "hard_accel_t"),
    ("low_ttc", "low_ttc_t"),
])
def test_every_injected_event_is_detected_once(synthetic, event_type, key):
    spark, truth = synthetic
    ev = spark.table("events").where(f"event_type = '{event_type}'").toPandas()
    expected = {seg: gt[key] for seg, gt in truth.items() if key in gt}

    assert set(ev.segment_id) == set(expected), "false positives or misses"
    assert len(ev) == len(expected), "an event was split or duplicated"
    for _, e in ev.iterrows():
        # allow for smoothing window / TTC-threshold latency
        assert abs(e.start_t - expected[e.segment_id]) < 1.0


def test_compound_scenarios_match_overlapping_events(synthetic):
    spark, truth = synthetic
    comp = spark.table("compound_scenarios").toPandas()
    expected = {s for s, gt in truth.items()
                if "hard_brake_t" in gt and "low_ttc_t" in gt
                and abs(gt["hard_brake_t"] - gt["low_ttc_t"]) < 3.0}
    assert set(comp.segment_id) == expected


def test_rates_are_exposure_normalised(synthetic):
    spark, _ = synthetic
    fm = spark.table("gold_fleet_metrics").toPandas()
    for _, r in fm.iterrows():
        assert r.per_1k_miles == pytest.approx(1000 * r.events / r.miles)
        assert r.per_1k_miles_lo <= r.per_1k_miles <= r.per_1k_miles_hi


def test_dq_checks_pass_on_clean_data(synthetic):
    spark, _ = synthetic
    dq = spark.table("gold_dq_report").toPandas()
    assert (dq.status == "PASS").all(), dq
