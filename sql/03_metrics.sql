-- =====================================================================
-- GOLD: safety metrics normalised by exposure (miles driven)
--   Raw event counts are meaningless without exposure: a car that drove
--   10x more will have ~10x more events. Always report a RATE.
-- =====================================================================

-- Per-segment summary (the grain the dashboard drills into)
CREATE OR REPLACE TEMP VIEW gold_segment_metrics AS
WITH exposure AS (
    SELECT segment_id,
           SUM(dist_m) / 1609.344D          AS miles,
           COUNT(*) / 600.0D                AS minutes,
           AVG(speed_mps) * 2.23694        AS avg_speed_mph,
           AVG(CASE WHEN lead_dist_m IS NOT NULL THEN 1.0 ELSE 0.0 END) AS lead_coverage
    FROM silver_timeline
    GROUP BY segment_id
),
counts AS (
    SELECT segment_id,
           SUM(CASE WHEN event_type = 'hard_brake' THEN 1 ELSE 0 END) AS hard_brakes,
           SUM(CASE WHEN event_type = 'hard_accel' THEN 1 ELSE 0 END) AS hard_accels,
           SUM(CASE WHEN event_type = 'low_ttc'    THEN 1 ELSE 0 END) AS low_ttc_events,
           MAX(severity) AS max_severity
    FROM events
    GROUP BY segment_id
)
SELECT s.segment_id, s.route_id, s.vehicle, s.drive_date, s.segment_num,
       e.miles, e.minutes, e.avg_speed_mph, e.lead_coverage,
       COALESCE(c.hard_brakes, 0)    AS hard_brakes,
       COALESCE(c.hard_accels, 0)    AS hard_accels,
       COALESCE(c.low_ttc_events, 0) AS low_ttc_events,
       COALESCE(c.max_severity, 0.0) AS max_severity
FROM bronze_segments s
JOIN exposure e     ON e.segment_id = s.segment_id
LEFT JOIN counts c  ON c.segment_id = s.segment_id;

-- Long format: one row per (vehicle, event_type) with a rate + 95% CI.
-- Events are ~Poisson, so SE(count) ~ sqrt(count). Normal approximation;
-- it's rough for small counts - which is exactly why we show the interval.
CREATE OR REPLACE TEMP VIEW gold_fleet_metrics AS
WITH per_vehicle AS (
    SELECT vehicle,
           COUNT(*)            AS segments,
           SUM(miles)          AS miles,
           SUM(hard_brakes)    AS hard_brake,
           SUM(hard_accels)    AS hard_accel,
           SUM(low_ttc_events) AS low_ttc
    FROM gold_segment_metrics
    GROUP BY vehicle
),
long AS (
    SELECT vehicle, segments, miles, 'hard_brake' AS event_type, hard_brake AS events FROM per_vehicle
    UNION ALL
    SELECT vehicle, segments, miles, 'hard_accel', hard_accel FROM per_vehicle
    UNION ALL
    SELECT vehicle, segments, miles, 'low_ttc', low_ttc FROM per_vehicle
)
SELECT vehicle, event_type, segments, miles, events,
       1000.0 * events / NULLIF(miles, 0) AS per_1k_miles,
       1000.0 * GREATEST(0, events - 1.96 * SQRT(events)) / NULLIF(miles, 0) AS per_1k_miles_lo,
       1000.0 * (events + 1.96 * SQRT(events)) / NULLIF(miles, 0)            AS per_1k_miles_hi,
       -- "miles per event", the framing Tesla's Vehicle Safety Report uses
       miles / NULLIF(events, 0) AS miles_per_event
FROM long;

-- Daily trend per vehicle
CREATE OR REPLACE TEMP VIEW gold_daily_metrics AS
SELECT drive_date, vehicle,
       SUM(miles) AS miles,
       SUM(hard_brakes)    AS hard_brakes,
       SUM(low_ttc_events) AS low_ttc_events,
       1000.0 * SUM(hard_brakes)    / NULLIF(SUM(miles), 0) AS hard_brakes_per_1k_miles,
       1000.0 * SUM(low_ttc_events) / NULLIF(SUM(miles), 0) AS low_ttc_per_1k_miles
FROM gold_segment_metrics
GROUP BY drive_date, vehicle;
