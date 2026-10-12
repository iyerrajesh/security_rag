"""Tunable constants for the fleet simulator. Units: metres, seconds, m/s, m/s^2."""

from dataclasses import dataclass

from schemas import ObjectType


@dataclass(frozen=True)
class Release:
    software_version: str
    perception_version: str
    firmware_version: str
    deployment_id: str


BASELINE_RELEASE = Release("4.6", "2.0.3", "3.1", "D-4.6-GA")
# Ships firmware 3.2, which intermittently degrades the camera: the planted root
# cause the platform should later rediscover.
FAULTY_RELEASE = Release("4.7", "2.1.0", "3.2", "D-4.7-ROLLOUT")
INITIAL_FAULTY_SHARE = 0.3

PHYSICS_DT = 0.1
HEALTH_INTERVAL = 1.0

# Starting area (San Francisco)
GPS_LAT_RANGE = (37.70, 37.80)
GPS_LON_RANGE = (-122.50, -122.39)
METRES_PER_DEGREE = 111_111.0

# Ego dynamics and planner
CRUISE_SPEED_RANGE = (11.0, 16.0)
MAX_ACCEL = 2.5
COMFORT_DECEL = 3.0
MAX_DECEL = 8.0
MAX_DECEL_RAIN = 6.0
EMERGENCY_DECEL_THRESHOLD = 4.5  # required decel above this triggers emergency braking
STOP_MARGIN = 3.0
FOLLOW_MARGIN = 6.0
PATH_HALF_WIDTH = 1.8
PEDESTRIAN_HAZARD_HALF_WIDTH = 4.0
CONSTRUCTION_SPEED = 8.0
SENSOR_FAILURE_SPEED = 5.0
RAIN_SPEED_FACTOR = 0.8
LANE_CHANGE_DURATION = 4.0
LANE_CHANGE_STEERING = 25.0
BATTERY_DRAIN_PER_M = 0.0002

# Perception
SENSOR_RANGE = 80.0
TRUST_THRESHOLD = 0.5  # planner ignores detections below this confidence
DETECTION_FLOOR = 0.25  # detections below this are not reported at all
CONFIDENCE_NOISE = 0.04
BASE_CONFIDENCE = {
    ObjectType.PEDESTRIAN: 0.90,
    ObjectType.VEHICLE: 0.95,
    ObjectType.CYCLIST: 0.88,
    ObjectType.MOTORCYCLE: 0.90,
    ObjectType.TRAFFIC_CONE: 0.85,
    ObjectType.TRAFFIC_SIGN: 0.93,
    ObjectType.ANIMAL: 0.80,
    ObjectType.UNKNOWN: 0.55,
}
VULNERABLE = {ObjectType.PEDESTRIAN, ObjectType.CYCLIST, ObjectType.ANIMAL}
DEGRADED_CAMERA_FACTOR = 0.6
LIDAR_FAILED_FACTOR = 0.9
RAIN_CONFIDENCE_FACTOR = 0.85
NIGHT_CONFIDENCE_FACTOR = 0.80  # vulnerable road users
NIGHT_CONFIDENCE_FACTOR_OTHER = 0.92
# Lidar confirms obstacles at close range (and is the only source when the camera fails).
LIDAR_RANGE = 20.0
LIDAR_CONFIDENCE = 0.62

# Camera degradation episodes, starts per vehicle-second, keyed by firmware version
CAMERA_DEGRADE_RATE = {"3.2": 0.05}
CAMERA_DEGRADE_RATE_DEFAULT = 0.002
CAMERA_DEGRADE_DURATION = (5.0, 20.0)

# Network latency, ms
NETWORK_LATENCY_MS = (20.0, 60.0)
DEGRADED_NETWORK_LATENCY_MS = (300.0, 1500.0)

# Background traffic
BACKGROUND_SPAWN_RATE = 0.8  # per vehicle-second
MAX_BACKGROUND_OBJECTS = 6
BACKGROUND_MIX = [
    (ObjectType.VEHICLE, 0.40),
    (ObjectType.PEDESTRIAN, 0.20),
    (ObjectType.TRAFFIC_SIGN, 0.15),
    (ObjectType.CYCLIST, 0.10),
    (ObjectType.TRAFFIC_CONE, 0.08),
    (ObjectType.MOTORCYCLE, 0.05),
    (ObjectType.ANIMAL, 0.02),
]
LEAD_VEHICLE_DECEL = 7.0
