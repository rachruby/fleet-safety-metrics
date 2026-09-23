-- =====================================================================
-- DATA QUALITY: automated checks that gate the pipeline
--   If the inputs are broken, the safety metric is wrong - and a wrong
--   safety metric is worse than no metric. Each check reports how many
--   units failed, the failure rate, and PASS/FAIL against a threshold.
-- =====================================================================

CREATE OR REPLACE TEMP VIEW dq_report AS
WITH seg_count AS (SELECT COUNT(*) AS n FROM bronze_segments),

-- 1) Segments with no CAN speed at all (can't compute exposure)
no_speed AS (
    SELECT COUNT(*) AS failing
    FROM bronze_segments s
    LEFT ANTI JOIN (SELECT DISTINCT segment_id FROM bronze_can_speed) c
      ON c.segment_id = s.segment_id
),

-- 2) Physically impossible speeds (negative or > 70 m/s ~ 157 mph)
bad_speed AS (
    SELECT SUM(CASE WHEN speed_mps < 0 OR speed_mps > 70 THEN 1 ELSE 0 END) AS failing,
           COUNT(*) AS total
    FROM bronze_can_speed
),

-- 3) Segments whose speed stream has a dropout longer than 1 s
gaps AS (
    SELECT segment_id, MAX(dt) AS max_gap_s
    FROM (
        SELECT segment_id, t - LAG(t) OVER (PARTITION BY segment_id ORDER BY t) AS dt
        FROM bronze_can_speed
    ) x
    GROUP BY segment_id
),
gap_fail AS (
    SELECT SUM(CASE WHEN max_gap_s > ${max_dropout_s} THEN 1 ELSE 0 END) AS failing,
           COUNT(*) AS total
    FROM gaps
),

-- 4) Silver rows missing steering after the join
steer_null AS (
    SELECT SUM(CASE WHEN steer_deg IS NULL THEN 1 ELSE 0 END) AS failing, COUNT(*) AS total
    FROM silver_timeline
),

-- 5) Segments with no radar lead coverage at all (TTC metric blind)
no_radar AS (
    SELECT SUM(CASE WHEN lead_coverage = 0 THEN 1 ELSE 0 END) AS failing, COUNT(*) AS total
    FROM gold_segment_metrics
)

SELECT 'segments_without_speed' AS check_name, failing, (SELECT n FROM seg_count) AS total,
       CAST(${thr_no_speed} AS DOUBLE) AS max_fail_rate FROM no_speed
UNION ALL SELECT 'impossible_speed_samples',   failing, total, CAST(${thr_bad_speed} AS DOUBLE)  FROM bad_speed
UNION ALL SELECT 'segments_with_speed_dropout', failing, total, CAST(${thr_dropout} AS DOUBLE)   FROM gap_fail
UNION ALL SELECT 'silver_rows_missing_steering', failing, total, CAST(${thr_steer_null} AS DOUBLE) FROM steer_null
UNION ALL SELECT 'segments_without_radar_lead', failing, total, CAST(${thr_no_radar} AS DOUBLE)  FROM no_radar;

CREATE OR REPLACE TEMP VIEW gold_dq_report AS
SELECT check_name, failing, total,
       failing / NULLIF(CAST(total AS DOUBLE), 0) AS fail_rate,
       max_fail_rate,
       CASE WHEN COALESCE(failing / NULLIF(CAST(total AS DOUBLE), 0), 0) <= max_fail_rate
            THEN 'PASS' ELSE 'FAIL' END AS status
FROM dq_report;
