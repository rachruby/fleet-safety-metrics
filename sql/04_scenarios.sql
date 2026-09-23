-- =====================================================================
-- SCENARIO MINING: find the moments worth sending to a training/eval set
--   Single events are common; *combinations* are interesting. A hard
--   brake that happens while TTC to the lead car is low is a real
--   "braked for a closing lead vehicle" scenario, not just a bumpy stop.
-- =====================================================================

-- Interval-overlap join: two events overlap (within a tolerance) when
--   a.start <= b.end + tol  AND  b.start <= a.end + tol
CREATE OR REPLACE TEMP VIEW compound_scenarios AS
SELECT CONCAT(b.event_id, '+ttc') AS scenario_id,
       b.segment_id,
       'brake_for_closing_lead'   AS scenario_type,
       LEAST(b.start_t, l.start_t)    AS start_t,
       GREATEST(b.end_t, l.end_t)     AS end_t,
       b.peak_value AS peak_decel_mps2,
       l.peak_value AS min_ttc_s,
       -- both dimensions matter; take the mean of the two severities
       (b.severity + l.severity) / 2.0 AS severity
FROM events b
JOIN events l
  ON  b.segment_id = l.segment_id
  AND b.event_type = 'hard_brake'
  AND l.event_type = 'low_ttc'
  AND b.start_t <= l.end_t + ${overlap_tolerance_s}
  AND l.start_t <= b.end_t + ${overlap_tolerance_s};

-- Labeling queue: top-N most severe per scenario type, with context.
-- A curator (or an auto-labeler) works through this list.
CREATE OR REPLACE TEMP VIEW gold_scenarios AS
WITH candidates AS (
    SELECT event_id AS scenario_id, segment_id, event_type AS scenario_type,
           start_t, end_t, severity,
           peak_value AS peak_value, avg_speed_mps
    FROM events
    UNION ALL
    SELECT c.scenario_id, c.segment_id, c.scenario_type,
           c.start_t, c.end_t, c.severity,
           c.peak_decel_mps2 AS peak_value, NULL AS avg_speed_mps
    FROM compound_scenarios c
),
ranked AS (
    SELECT c.*, s.vehicle, s.route_id, s.drive_date,
           ROW_NUMBER() OVER (PARTITION BY c.scenario_type ORDER BY c.severity DESC, c.scenario_id) AS rank_in_type
    FROM candidates c
    JOIN bronze_segments s ON s.segment_id = c.segment_id
)
SELECT *
FROM ranked
WHERE rank_in_type <= ${top_n_per_type};
