-- Pedestrian detection confidence by firmware version
--
-- The planner trusts a detection only at confidence >= 0.5. Pedestrians seen below that are "weak": the vehicle
-- sees them but will not act on them until lidar confirms them at about 20 m. This is the middle link of the
-- simulator's planted chain (firmware 3.2 -> camera degradation -> lower pedestrian confidence -> braking).
WITH versions AS (
    SELECT vehicle_id,
           "timestamp" AS valid_from,
           lead("timestamp") OVER (PARTITION BY vehicle_id ORDER BY "timestamp") AS valid_to,
           firmware_version
    FROM curated.software_deployments
)
SELECT v.firmware_version,
       count(DISTINCT p.vehicle_id) AS vehicles,
       count(*) AS pedestrian_detections,
       round(avg(p.confidence), 3) AS avg_confidence,
       count_if(p.confidence < 0.5) AS weak_detections,
       round(CAST(100 AS double) * count_if(p.confidence < 0.5) / count(*), 1) AS weak_pct
FROM curated.vehicle_perception p
JOIN versions v
  ON p.vehicle_id = v.vehicle_id
 AND p."timestamp" >= v.valid_from
 AND (v.valid_to IS NULL OR p."timestamp" < v.valid_to)
WHERE p.object_type = 'pedestrian'
GROUP BY v.firmware_version
ORDER BY v.firmware_version
