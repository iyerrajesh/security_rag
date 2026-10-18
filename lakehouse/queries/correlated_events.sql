-- Correlated perception, planning and telemetry rows (Milestone 1 exit query)
--
-- For each pedestrian detection: the planner's decision and the vehicle's motion within one second of it.
-- Planning and telemetry rows nearest in time are chosen with min_by.
SELECT p.vehicle_id,
       p."timestamp" AS detected_at,
       p.object_type,
       round(p.confidence, 2) AS confidence,
       round(p.x, 1) AS distance_m,
       min_by(pl.planned_action, abs(date_diff('millisecond', pl."timestamp", p."timestamp"))) AS planned_action,
       min_by(pl.emergency_braking, abs(date_diff('millisecond', pl."timestamp", p."timestamp"))) AS emergency_braking,
       round(min_by(t.speed, abs(date_diff('millisecond', t."timestamp", p."timestamp"))), 1) AS speed_mps,
       round(min_by(t.braking, abs(date_diff('millisecond', t."timestamp", p."timestamp"))), 2) AS brake_pressure
FROM curated.vehicle_perception p
JOIN curated.vehicle_planning pl
  ON pl.vehicle_id = p.vehicle_id
 AND pl."timestamp" BETWEEN p."timestamp" - INTERVAL '1' SECOND AND p."timestamp" + INTERVAL '1' SECOND
JOIN curated.vehicle_telemetry t
  ON t.vehicle_id = p.vehicle_id
 AND t."timestamp" BETWEEN p."timestamp" - INTERVAL '1' SECOND AND p."timestamp" + INTERVAL '1' SECOND
WHERE p.object_type = 'pedestrian'
GROUP BY p.vehicle_id, p."timestamp", p.object_id, p.object_type, p.confidence, p.x
ORDER BY detected_at, p.vehicle_id
LIMIT 20
