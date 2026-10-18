-- Pedestrian detection failures: perception JOIN planning JOIN telemetry
--
-- A failure is a pedestrian within 40 m seen with confidence below the planner's 0.5 trust threshold, followed
-- by an emergency brake on the same vehicle within 3 seconds. Each failure is joined to the telemetry nearest
-- the braking moment, to show what the vehicle was doing.
--
-- Correlation is on vehicle_id plus a time window, never on arrival order: the three topics are not ordered
-- relative to each other, and a vehicle reports each stream at its own rate.
--
-- {{and_window}} is empty when run from the command line; the benchmarks fill it with a time range to measure
-- partition pruning.
WITH weak AS (
    SELECT vehicle_id, "timestamp" AS seen_at, object_id, confidence, x
    FROM curated.vehicle_perception
    WHERE object_type = 'pedestrian' AND confidence < 0.5 AND x BETWEEN 0 AND 40 {{and_window}}
),
braking AS (
    SELECT vehicle_id, "timestamp" AS braked_at, planned_action
    FROM curated.vehicle_planning
    WHERE emergency_braking {{and_window}}
)
SELECT w.vehicle_id,
       w.seen_at,
       w.object_id,
       round(w.confidence, 2) AS confidence,
       round(w.x, 1) AS distance_m,
       b.braked_at,
       round(date_diff('millisecond', w.seen_at, b.braked_at) / 1000.0, 1) AS delay_s,
       b.planned_action,
       round(min_by(t.speed, abs(date_diff('millisecond', t."timestamp", b.braked_at))), 1) AS speed_mps,
       round(min_by(t.acceleration, abs(date_diff('millisecond', t."timestamp", b.braked_at))), 1) AS accel_mps2,
       round(min_by(t.braking, abs(date_diff('millisecond', t."timestamp", b.braked_at))), 2) AS brake_pressure
FROM weak w
JOIN braking b
  ON b.vehicle_id = w.vehicle_id
 AND b.braked_at BETWEEN w.seen_at AND w.seen_at + INTERVAL '3' SECOND
JOIN curated.vehicle_telemetry t
  ON t.vehicle_id = b.vehicle_id
 AND t."timestamp" BETWEEN b.braked_at - INTERVAL '1' SECOND AND b.braked_at + INTERVAL '1' SECOND
GROUP BY w.vehicle_id, w.seen_at, w.object_id, w.confidence, w.x, b.braked_at, b.planned_action
ORDER BY w.seen_at, w.vehicle_id
LIMIT 50
