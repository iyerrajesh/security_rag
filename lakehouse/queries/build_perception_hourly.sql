-- Build curated.perception_hourly: pedestrian-and-other detections pre-aggregated per vehicle, hour and object type
--
-- A pre-aggregation trades storage and a refresh step for much cheaper dashboards: a question about confidence
-- per hour reads a few hundred summary rows instead of every detection. The grain keeps vehicle_id so it can
-- still answer per-vehicle questions; the sums (not averages) can be re-aggregated to any coarser grain.
-- Rebuild with: DROP TABLE IF EXISTS curated.perception_hourly, then run this.
CREATE TABLE curated.perception_hourly
WITH (partitioning = ARRAY['day(hour)'])
AS
SELECT date_trunc('hour', "timestamp") AS hour,
       vehicle_id,
       object_type,
       count(*) AS detections,
       sum(confidence) AS confidence_sum,
       count_if(confidence < 0.5) AS weak_detections
FROM curated.vehicle_perception
GROUP BY 1, 2, 3
