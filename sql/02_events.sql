-- =====================================================================
-- EVENTS: turn per-sample threshold crossings into discrete events
--   A 1.2 s hard brake is ~12 rows above threshold; it should count as
--   ONE event. This is the classic "gaps and islands" pattern.
-- =====================================================================

-- 1) Flag samples. UNION ALL (not CASE) because one sample can be both a
--    hard brake AND a low-TTC moment; each type gets its own row.
CREATE OR REPLACE TEMP VIEW event_samples AS
SELECT segment_id, bucket, 'hard_brake' AS event_type, accel_mps2 AS signal, speed_mps
FROM silver_timeline
WHERE accel_mps2 <= -${hard_brake_mps2} AND speed_mps >= ${min_speed_mps}
UNION ALL
SELECT segment_id, bucket, 'hard_accel', accel_mps2, speed_mps
FROM silver_timeline
WHERE accel_mps2 >= ${hard_accel_mps2} AND speed_mps >= ${min_speed_mps}
UNION ALL
SELECT segment_id, bucket, 'low_ttc', ttc_s, speed_mps
FROM silver_timeline
WHERE ttc_s < ${ttc_threshold_s} AND speed_mps >= ${min_speed_mps};

-- 2) Gaps and islands: consecutive buckets share the same
--    (bucket - row_number) value, so that difference is an island id.
--      bucket: 100 101 102 | 107 108
--      rn:       1   2   3 |   4   5
--      diff:    99  99  99 | 103 103   -> two islands
CREATE OR REPLACE TEMP VIEW events AS
WITH islands AS (
    SELECT *,
           bucket - ROW_NUMBER() OVER (
               PARTITION BY segment_id, event_type ORDER BY bucket
           ) AS island_id
    FROM event_samples
),
grouped AS (
    SELECT segment_id,
           event_type,
           MIN(bucket)                     AS start_bucket,
           MAX(bucket)                     AS end_bucket,
           (MAX(bucket) - MIN(bucket) + 1) / 10.0D AS duration_s,
           -- "peak" = most severe value in the event
           CASE event_type WHEN 'hard_accel' THEN MAX(signal) ELSE MIN(signal) END AS peak_value,
           AVG(speed_mps)                  AS avg_speed_mps
    FROM islands
    GROUP BY segment_id, event_type, island_id
    HAVING COUNT(*) >= ${min_event_samples}
)
SELECT CONCAT(segment_id, '#', event_type, '#', CAST(start_bucket AS STRING)) AS event_id,
       segment_id,
       event_type,
       start_bucket / 10.0D AS start_t,
       end_bucket / 10.0D   AS end_t,
       duration_s,
       peak_value,
       avg_speed_mps,
       -- normalised 0..1 severity so different event types can be ranked together
       CASE event_type
           WHEN 'hard_brake' THEN LEAST(1.0, -peak_value / ${severe_brake_mps2})
           WHEN 'hard_accel' THEN LEAST(1.0,  peak_value / ${severe_accel_mps2})
           WHEN 'low_ttc'    THEN GREATEST(0.0, 1.0 - peak_value / ${ttc_threshold_s})
       END AS severity
FROM grouped;
