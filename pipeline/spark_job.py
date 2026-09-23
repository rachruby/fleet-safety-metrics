"""
Spark job: bronze Parquet -> silver timeline -> gold safety metrics.

All transformation logic lives in sql/*.sql so it can be read, reviewed and
tested as SQL. This file only wires inputs, parameters and outputs.

Usage:
    python pipeline/spark_job.py --bronze data/bronze --out data --strict
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from string import Template

from pyspark.sql import SparkSession

log = logging.getLogger("spark_job")
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Every threshold in one place. These are the knobs a metrics owner tunes
# (and should be able to justify in a review).
PARAMS = {
    # silver
    "lane_half_width_m": 1.8,        # ~half a 3.7 m highway lane
    "max_radar_range_m": 150,
    "max_gap_buckets": 5,            # don't differentiate across >0.5 s gaps
    "min_closing_speed_mps": 0.5,    # avoid TTC -> infinity when barely closing
    # events
    "hard_brake_mps2": 3.0,          # ~0.3 g, a common telematics threshold
    "hard_accel_mps2": 2.5,
    "ttc_threshold_s": 3.0,
    "min_speed_mps": 5.0,            # ignore parking-lot creeping
    "min_event_samples": 3,          # event must last >= 0.3 s
    "severe_brake_mps2": 6.0,        # severity = 1.0 at ~0.6 g
    "severe_accel_mps2": 4.0,
    # scenarios
    "overlap_tolerance_s": 2.0,
    "top_n_per_type": 50,
    # data quality thresholds (max allowed failure rate)
    "max_dropout_s": 1.0,
    "thr_no_speed": 0.0,
    "thr_bad_speed": 0.001,
    "thr_dropout": 0.10,
    "thr_steer_null": 0.05,
    "thr_no_radar": 0.50,
}

BRONZE_TABLES = ["segments", "can_speed", "can_steering", "radar", "gnss"]

OUTPUTS = {  # view -> output path relative to --out
    "silver_timeline": "silver/timeline",
    "events": "gold/events",
    "gold_segment_metrics": "gold/segment_metrics",
    "gold_fleet_metrics": "gold/fleet_metrics",
    "gold_daily_metrics": "gold/daily_metrics",
    "gold_scenarios": "gold/scenarios",
    "gold_dq_report": "gold/dq_report",
}


def build_spark(app: str = "fleet-safety-metrics") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")  # small local data; default 200 is overkill
        .getOrCreate()
    )


def split_statements(sql: str) -> list[str]:
    # strip -- comments, then split on ';'
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def run_sql_file(spark: SparkSession, path: Path, params: dict = PARAMS) -> None:
    rendered = Template(path.read_text()).substitute({k: str(v) for k, v in params.items()})
    for stmt in split_statements(rendered):
        spark.sql(stmt)
    log.info("ran %s", path.name)


def run_sql_files(spark: SparkSession, params: dict = PARAMS, sql_dir: Path = SQL_DIR) -> None:
    for path in sorted(sql_dir.glob("*.sql")):
        run_sql_file(spark, path, params)


def register_bronze(spark: SparkSession, bronze: Path) -> None:
    for t in BRONZE_TABLES:
        p = bronze / t
        if not p.exists():
            raise SystemExit(f"Missing bronze table {p}. Run pipeline/ingest.py first.")
        spark.read.parquet(str(p)).createOrReplaceTempView(f"bronze_{t}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", type=Path, default=Path("data/bronze"))
    ap.add_argument("--out", type=Path, default=Path("data"))
    ap.add_argument("--strict", action="store_true", help="exit non-zero if any DQ check FAILs")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    register_bronze(spark, args.bronze)
    run_sql_files(spark)

    # silver_timeline feeds every downstream view; cache so it's computed once
    spark.table("silver_timeline").cache()

    # DQ first: if the gate fails we don't publish metrics
    dq = spark.table("gold_dq_report").toPandas()
    log.info("Data quality report:\n%s", dq.to_string(index=False))
    failed = dq[dq.status == "FAIL"]

    for view, rel in OUTPUTS.items():
        if args.strict and not failed.empty and view != "gold_dq_report":
            continue
        spark.table(view).write.mode("overwrite").parquet(str(args.out / rel))
        log.info("wrote %s -> %s", view, args.out / rel)

    fleet = spark.table("gold_fleet_metrics").orderBy("vehicle", "event_type").toPandas()
    log.info("Fleet metrics:\n%s", fleet.round(3).to_string(index=False))

    spark.stop()
    if args.strict and not failed.empty:
        log.error("DQ gate FAILED: %s", ", ".join(failed.check_name))
        sys.exit(1)


if __name__ == "__main__":
    main()
