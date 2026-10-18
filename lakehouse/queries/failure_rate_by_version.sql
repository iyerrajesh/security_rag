-- Failure rate by software, perception and firmware version
--
-- A vehicle runs one version at a time, and `software_deployments` records when each version started. So an
-- event belongs to the version whose interval [valid_from, valid_to) contains its timestamp (an as-of join).
-- "Failure" is two measurable things per 100 events:
--   * emergency braking, from the planner (curated.vehicle_planning)
--   * a camera that was not `ok`, from health (curated.vehicle_health)
-- Events before a vehicle's first recorded version have no interval and are left out.
WITH versions AS (
    SELECT vehicle_id,
           "timestamp" AS valid_from,
           lead("timestamp") OVER (PARTITION BY vehicle_id ORDER BY "timestamp") AS valid_to,
           software_version, perception_version, firmware_version
    FROM curated.software_deployments
),
braking AS (
    SELECT v.software_version, v.perception_version, v.firmware_version,
           count(DISTINCT p.vehicle_id) AS vehicles,
           count(*) AS planning_events,
           count_if(p.emergency_braking) AS emergency_brakes
    FROM curated.vehicle_planning p
    JOIN versions v
      ON p.vehicle_id = v.vehicle_id
     AND p."timestamp" >= v.valid_from
     AND (v.valid_to IS NULL OR p."timestamp" < v.valid_to)
    GROUP BY 1, 2, 3
),
cameras AS (
    SELECT v.software_version, v.perception_version, v.firmware_version,
           count(*) AS health_events,
           count_if(h.camera_status <> 'ok') AS camera_faults
    FROM curated.vehicle_health h
    JOIN versions v
      ON h.vehicle_id = v.vehicle_id
     AND h."timestamp" >= v.valid_from
     AND (v.valid_to IS NULL OR h."timestamp" < v.valid_to)
    GROUP BY 1, 2, 3
)
SELECT software_version, perception_version, firmware_version,
       b.vehicles,
       b.planning_events,
       b.emergency_brakes,
       round(CAST(100 AS double) * b.emergency_brakes / b.planning_events, 2) AS emergency_brakes_per_100,
       c.health_events,
       c.camera_faults,
       round(CAST(100 AS double) * c.camera_faults / c.health_events, 2) AS camera_faults_per_100
FROM braking b
JOIN cameras c USING (software_version, perception_version, firmware_version)
ORDER BY firmware_version, software_version
