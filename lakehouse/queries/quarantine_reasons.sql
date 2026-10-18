-- Why records were quarantined, by reason and source topic
SELECT reason, source_topic, count(*) AS records, count(DISTINCT vehicle_id) AS vehicles
FROM quarantine.vehicle
GROUP BY reason, source_topic
ORDER BY records DESC
