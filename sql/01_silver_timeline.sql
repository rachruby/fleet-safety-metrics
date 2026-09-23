-- =====================================================================
-- SILVER: one clean 10 Hz timeline per segment
--   Sensors arrive at different rates (CAN speed ~89 Hz, radar bursts,
--   GNSS 0.5 Hz). We snap everything onto a common 100 ms grid so the
--   signals can be joined row-for-row.
-- =====================================================================

-- CAN speed -> 10 Hz (bucket = 100 ms slot since device boot)
CREATE OR REPLACE TEMP VIEW speed_10hz AS
SELECT segment_id,
       CAST(FLOOR(t * 10) AS BIGINT) AS bucket,
       AVG(speed_mps)                AS speed_mps
FROM bronze_can_speed
WHERE speed_mps IS NOT NULL AND NOT isnan(speed_mps)
GROUP BY segment_id, CAST(FLOOR(t * 10) AS BIGINT);

-- Steering -> 10 Hz
CREATE OR REPLACE TEMP VIEW steer_10hz AS
SELECT segment_id,
       CAST(FLOOR(t * 10) AS BIGINT) AS bucket,
       AVG(steer_deg)                AS steer_deg
FROM bronze_can_steering
GROUP BY segment_id, CAST(FLOOR(t * 10) AS BIGINT);

-- Lead vehicle = closest radar return inside our lane, per bucket.
-- ROW_NUMBER() keeps exactly one row per (segment, bucket).
CREATE OR REPLACE TEMP VIEW lead_10hz AS
SELECT segment_id, bucket, lead_dist_m, lead_rel_mps, lead_track_id
FROM (
    SELECT segment_id,
           CAST(FLOOR(t * 10) AS BIGINT) AS bucket,
           fwd_m    AS lead_dist_m,
           rel_mps  AS lead_rel_mps,
           track_id AS lead_track_id,
           ROW_NUMBER() OVER (
               PARTITION BY segment_id, CAST(FLOOR(t * 10) AS BIGINT)
               ORDER BY fwd_m
           ) AS rn
    FROM bronze_radar
    WHERE ABS(lat_m) < ${lane_half_width_m}
      AND fwd_m > 0 AND fwd_m < ${max_radar_range_m}
      AND NOT isnan(rel_mps)
) ranked
WHERE rn = 1;

-- Join the streams and derive acceleration + time-to-collision.
CREATE OR REPLACE TEMP VIEW silver_timeline AS
WITH joined AS (
    SELECT s.segment_id, s.bucket, s.speed_mps,
           st.steer_deg,
           l.lead_dist_m, l.lead_rel_mps, l.lead_track_id
    FROM speed_10hz s
    LEFT JOIN steer_10hz st ON st.segment_id = s.segment_id AND st.bucket = s.bucket
    LEFT JOIN lead_10hz  l  ON l.segment_id  = s.segment_id AND l.bucket  = s.bucket
),
smoothed AS (
    -- 5-sample (0.5 s) centered moving average: differentiating raw CAN
    -- speed amplifies quantization noise into fake "hard brakes".
    SELECT *,
           AVG(speed_mps) OVER (
               PARTITION BY segment_id ORDER BY bucket
               ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
           ) AS speed_smooth_mps
    FROM joined
),
derived AS (
    SELECT *,
           bucket - LAG(bucket) OVER w AS gap_buckets,
           (speed_smooth_mps - LAG(speed_smooth_mps) OVER w)
             / ((bucket - LAG(bucket) OVER w) / 10.0D) AS raw_accel_mps2
    FROM smoothed
    WINDOW w AS (PARTITION BY segment_id ORDER BY bucket)
)
SELECT segment_id,
       bucket,
       bucket / 10.0D AS t,
       speed_mps,
       speed_smooth_mps,
       steer_deg,
       lead_dist_m,
       lead_rel_mps,
       lead_track_id,
       -- never differentiate across a sensor dropout
       CASE WHEN gap_buckets <= ${max_gap_buckets} THEN raw_accel_mps2 END AS accel_mps2,
       -- TTC only exists while we are closing on the lead (rel < 0)
       CASE WHEN lead_rel_mps < -${min_closing_speed_mps}
            THEN lead_dist_m / (-lead_rel_mps) END AS ttc_s,
       -- distance travelled in this 100 ms slot; buckets missing from a
       -- dropout are simply not counted (conservative mileage)
       speed_mps * 0.1 AS dist_m
FROM derived;
