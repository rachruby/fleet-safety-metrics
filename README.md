# Fleet Safety Metrics

**Live demo:** https://fleet-safety-metrics.onrender.com/

I started working on the project to figure out how different driving scenarios could result in a visualized dashboard. So I quickly "jammed" a bunch of datasets/logs retrieved to filter out a bunch of metrics. A Spark SQL pipeline that turns raw vehicle logs from the public **comma2k19** dataset (33 hours of real highway driving, CAN bus + radar + GNSS) into exposure-normalised safety metrics, mines the most interesting driving scenarios, and serves the results on a dashboard.

```
comma2k19 .npy logs ──ingest──▶ BRONZE (Parquet, long format)
                                   │  Spark SQL
                                   ▼
                     SILVER  10 Hz timeline per segment
                             speed · accel · steering · lead car · TTC
                                   │
                ┌──────────────────┼──────────────────┬─────────────────┐
                ▼                  ▼                  ▼                 ▼
          GOLD events      GOLD metrics        GOLD scenarios     GOLD DQ report
       (gaps & islands)  (per 1,000 miles,   (compound events,   (pipeline gate)
                          95% intervals)      top-N queue)
                                   │
                                   ▼
                        Streamlit dashboard
```

## Metrics

| Event | Definition (defaults in `pipeline/spark_job.py → PARAMS`) |
|---|---|
| Hard brake | smoothed decel ≤ −3.0 m/s² (~0.3 g) for ≥ 0.3 s at ≥ 5 m/s |
| Hard acceleration | smoothed accel ≥ 2.5 m/s² for ≥ 0.3 s at ≥ 5 m/s |
| Low time-to-collision | in-lane lead vehicle, TTC = distance / closing speed < 3 s for ≥ 0.3 s |
| Braked for closing lead (compound) | a hard brake overlapping a low-TTC event (±2 s) |

Every rate is reported **per 1,000 miles** (and as miles per event), with an approximate 95% Poisson interval.

## Quick start

```bash
pip install -r requirements.txt          # needs Java 17+ for Spark

# 1) Try it instantly on synthetic data (same on-disk layout, injected events)
make synthetic
make dashboard DATA=data/synthetic

# 2) Run the tests (8 tests: SQL unit tests + end-to-end vs ground truth)
make test
```

### Running on real data

**The one-minute sample** (in the comma2k19 GitHub repo, ~50 MB including its video):

```bash
git clone --depth 1 --filter=blob:none --no-checkout https://github.com/commaai/comma2k19.git
cd comma2k19 && git checkout HEAD -- "Example_1" && cd ..
make ingest metrics RAW=comma2k19/Example_1
make dashboard
```

**The full dataset** (~100 GB, 10 chunks of ~10 GB each) is on Academic Torrents:
<https://academictorrents.com/details/65a2fbc964078aff62076ff4e103f18b951c5ddb>.
Start with one chunk, extract it under `data/raw/`, then run `make all`. Chunks 1–2 are a Toyota RAV4 and the rest are a Honda Civic, so two chunks let you compare vehicles. You only need the `processed_log/` folders, not the video.

## Deployment

The hosted dashboard doesn't run Spark. The pipeline runs offline, and only its small gold/silver output (`demo_data/`, about 1 MB) is deployed. This is the usual split: heavy batch compute happens elsewhere, and a light app serves the results.

- `render.yaml` is a Render Blueprint (free plan) that installs only `requirements-dashboard.txt`.
- `demo_data/source.json` controls the provenance banner. It's currently `synthetic`.

To publish **real** results, run the pipeline on comma2k19 chunks, then:
```bash
make demo-data   # copies data/gold + data/silver into demo_data/
```
Then set `"source"` in `demo_data/source.json` to describe the chunks you used, and push. Render redeploys automatically.

## Design decisions

- **Resample to a common 10 Hz grid before joining.** CAN speed arrives at about 89 Hz, radar in bursts, and GNSS at 0.5 Hz. Snapping all of them to 100 ms buckets lets the signals be joined row-for-row.
- **Smooth before differentiating.** CAN speed is quantised, and differentiating it raw produces spikes that look like hard brakes. A 0.5 s centred moving average (`AVG() OVER (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)`) removes them. Acceleration is also never computed across a sensor dropout.
- **Detect events with gaps-and-islands.** A 1.5 s brake spans about 15 rows above threshold but should count as one event. Subtracting `ROW_NUMBER()` from `bucket` gives consecutive rows a shared island ID (see the worked example in `sql/02_events.sql`).
- **Normalise by exposure.** Raw counts scale with miles driven, so every metric is a rate.
- **Show confidence intervals.** With few miles, rates are noisy. If the intervals for two vehicles overlap, the dashboard is not claiming one is safer than the other.
- **Radar sign convention.** The TTC calculation assumes negative relative speed means closing. I verified this on the real sample by comparing it with d(distance)/dt.
- **Data-quality gate.** With `--strict`, the job refuses to publish metrics if checks fail: missing speed, impossible values, dropouts, missing steering, or no radar coverage. A wrong safety metric is worse than none.
- **Keep all logic in SQL files.** They're reviewable, diffable, and unit-testable. All thresholds live in one `PARAMS` dict.

## Project layout

```
pipeline/ingest.py        numpy logs → bronze Parquet (parallel, incremental)
pipeline/spark_job.py     runs sql/*.sql in order, DQ gate, writes silver/gold
sql/01_silver_timeline.sql  resample, join, smooth, accel, TTC
sql/02_events.sql           threshold flags → gaps & islands → events + severity
sql/03_metrics.sql          per-segment, per-vehicle (with CI), daily metrics
sql/04_scenarios.sql        interval-overlap join for compound scenarios, top-N queue
sql/05_dq_checks.sql        data-quality checks with thresholds
dashboard/app.py          Streamlit + Plotly dashboard
tools/make_synthetic.py   synthetic data with ground-truth events (tests/demo only)
tests/                    pytest: SQL unit tests + end-to-end ground-truth tests
```

## Roadmap: making it more advanced

1. **Scale.** Run on all 10 chunks. Partition the silver table by `drive_date`, compact the small bronze files, and profile shuffle sizes in the Spark UI.
2. **Incremental processing.** Replace full rewrites with Delta Lake `MERGE`, so newly arrived segments are processed without recomputing history. Then try Spark Structured Streaming.
3. **Orchestration.** Build an Airflow (or Databricks Workflows) DAG, `ingest → metrics → DQ gate → publish`, with alerting on failure.
4. **More scenarios.** Detect lane changes from steering plus lateral radar; cut-ins from radar `new_track` appearing at a short in-lane distance; and stop-and-go waves.
5. **Geospatial hotspots.** Join events to GNSS, bucket locations with H3 hexagons, and map where hard brakes cluster along the I-280.
6. **Better statistics.** Compare vehicles with an exact Poisson rate-ratio test, and run a sensitivity analysis showing how rates move as the thresholds change.
7. **Learned triggers.** Score segments with an anomaly model and compare them against the rule-based events. Which interesting moments do the rules miss?
