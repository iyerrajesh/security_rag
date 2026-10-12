"""A simulated vehicle: ego dynamics, the world around it, perception, and a planner.

Each tick runs world -> perception -> planner -> dynamics, and the planner's
output is what moves the vehicle. That keeps the streams physically consistent:
a pedestrian trusted late forces a hard stop that shows up in perception
(confidence), planning (emergency_braking) and telemetry (deceleration).

Failure chain planted for later discovery:
  firmware 3.2 -> camera degradation episodes -> lower perception confidence
  -> pedestrians trusted only at lidar range -> emergency braking.
"""

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from schemas import (
    BaseEvent,
    HealthEvent,
    ObjectType,
    PerceptionEvent,
    PlannedAction,
    PlanningEvent,
    SensorStatus,
    SoftwareEvent,
    TelemetryEvent,
)

from . import config as C
from .scenarios import SPECS, Episode, Scenario

OBJECT_ID_PREFIX = {
    ObjectType.PEDESTRIAN: "P",
    ObjectType.VEHICLE: "OV",
    ObjectType.CYCLIST: "C",
    ObjectType.MOTORCYCLE: "M",
    ObjectType.TRAFFIC_CONE: "TC",
    ObjectType.TRAFFIC_SIGN: "TS",
    ObjectType.ANIMAL: "A",
    ObjectType.UNKNOWN: "U",
}


@dataclass
class WorldObject:
    object_id: str
    object_type: ObjectType
    x: float  # metres ahead of the ego vehicle
    y: float  # metres to the left of the ego vehicle
    vx: float  # ground speed along the ego heading
    vy: float  # ground speed to the left
    episode: Optional[Episode] = None  # scenario that spawned this object
    # Lead-vehicle braking manoeuvre (hard_braking scenario)
    brake_at: Optional[float] = None
    resume_at: Optional[float] = None
    resume_speed: float = 0.0


@dataclass
class Detection:
    obj: WorldObject
    object_type: ObjectType
    confidence: float


@dataclass
class Plan:
    action: PlannedAction
    target_speed: float
    emergency_braking: bool
    accel: float


class Vehicle:
    def __init__(self, vehicle_id: str, seed: int, sim_start: datetime,
                 faulty_share: float = C.INITIAL_FAULTY_SHARE, scenario_rate: float = 1.0):
        self.id = vehicle_id
        # Per-vehicle RNG: a vehicle behaves the same regardless of fleet size.
        self.rng = r = random.Random(f"{seed}:{vehicle_id}")
        self.sim_start = sim_start
        self.scenario_rate = scenario_rate

        self.release = C.FAULTY_RELEASE if r.random() < faulty_share else C.BASELINE_RELEASE
        self.cruise_speed = r.uniform(*C.CRUISE_SPEED_RANGE)
        self.speed = r.uniform(0.5, 1.0) * self.cruise_speed
        self.accel = 0.0
        self.steering = 0.0
        self.battery = r.uniform(40.0, 100.0)
        self.lat = r.uniform(*C.GPS_LAT_RANGE)
        self.lon = r.uniform(*C.GPS_LON_RANGE)
        self.heading = r.uniform(0.0, 2 * math.pi)
        self.memory = r.uniform(40.0, 55.0)

        self.objects: list[WorldObject] = []
        self.active: dict[Scenario, Episode] = {}
        self.finished: list[Episode] = []
        self.failed_sensor: Optional[str] = None  # "camera" | "lidar"
        self.lane_change_start: Optional[float] = None
        self._object_seq = 0
        self._trajectory_seq = 0
        self._last_action: Optional[PlannedAction] = None
        self.trajectory_id = self._next_trajectory_id()

        for scenario, spec in SPECS.items():
            if spec.initial_probability and r.random() < spec.initial_probability:
                self._start(scenario, 0.0)

    # ------------------------------------------------------------------ tick

    def boot_events(self) -> list[BaseEvent]:
        """Software inventory reported when the vehicle comes online."""
        return [self._software_event(0.0)]

    def step(self, t: float, dt: float, report: bool, report_health: bool) -> list[BaseEvent]:
        events: list[BaseEvent] = []
        self._update_scenarios(t, dt, events)
        self._update_world(t, dt)
        detections = self._perceive()
        plan = self._plan(detections, t)
        self._apply(plan, t, dt)
        if report:
            events.append(self._telemetry_event(t))
            events.extend(self._perception_event(t, d) for d in detections)
            events.append(self._planning_event(t, plan))
        if report_health:
            events.append(self._health_event(t, len(detections)))
        return events

    def close(self, t: float) -> list[Episode]:
        """End open episodes at sim end and return all ground-truth episodes."""
        for scenario in list(self.active):
            self._end(scenario, t)
        return self.finished

    # ------------------------------------------------------------- scenarios

    def _update_scenarios(self, t: float, dt: float, events: list[BaseEvent]) -> None:
        for scenario, ep in list(self.active.items()):
            if t >= ep.end_s:
                self._end(scenario, ep.end_s)

        for scenario, spec in SPECS.items():
            if scenario in self.active or not self._can_start(scenario):
                continue
            if self.rng.random() < spec.rate * self.scenario_rate * dt:
                self._start(scenario, t)
                if scenario is Scenario.SOFTWARE_DEPLOYMENT:
                    events.append(self._software_event(t))

        if Scenario.CAMERA_DEGRADATION not in self.active:
            rate = C.CAMERA_DEGRADE_RATE.get(self.release.firmware_version, C.CAMERA_DEGRADE_RATE_DEFAULT)
            if self.rng.random() < rate * dt:
                self._start(Scenario.CAMERA_DEGRADATION, t)

    def _can_start(self, scenario: Scenario) -> bool:
        if scenario is Scenario.SOFTWARE_DEPLOYMENT:
            return self.release != C.FAULTY_RELEASE
        if scenario is Scenario.HARD_BRAKING:
            return not any(abs(o.y) < C.PATH_HALF_WIDTH and o.x > 0 for o in self.objects)
        return True

    def _start(self, scenario: Scenario, t: float) -> None:
        r = self.rng
        spec = SPECS.get(scenario)
        if scenario is Scenario.CAMERA_DEGRADATION:
            duration: Optional[float] = r.uniform(*C.CAMERA_DEGRADE_DURATION)
        else:
            duration = r.uniform(*spec.duration) if spec.duration else None
        ep = Episode(scenario, self.id, t, t + duration if duration is not None else math.inf)

        if scenario is Scenario.PEDESTRIAN_CROSSING:
            side = r.choice((-1, 1))
            ped = self._spawn(ObjectType.PEDESTRIAN, x=r.uniform(35.0, 60.0), y=side * 5.0,
                              vx=0.0, vy=-side * r.uniform(1.1, 1.6), episode=ep)
            ep.details["object_id"] = ped.object_id
        elif scenario is Scenario.HARD_BRAKING:
            lead = self._spawn(ObjectType.VEHICLE, x=r.uniform(15.0, 30.0), y=0.0,
                               vx=self.speed, vy=0.0, episode=ep)
            lead.brake_at = t + r.uniform(1.0, 3.0)
            lead.resume_speed = self.cruise_speed + 4.0
            ep.details["object_id"] = lead.object_id
        elif scenario is Scenario.SENSOR_FAILURE:
            self.failed_sensor = r.choice(("camera", "lidar"))
            ep.details["sensor"] = self.failed_sensor
        elif scenario is Scenario.CONSTRUCTION_ZONE:
            x = r.uniform(20.0, 30.0)
            while x < C.SENSOR_RANGE:
                self._spawn(ObjectType.TRAFFIC_CONE, x=x, y=-r.uniform(2.4, 2.8), vx=0.0, vy=0.0)
                x += r.uniform(6.0, 10.0)
            self.lane_change_start = t
        elif scenario is Scenario.SOFTWARE_DEPLOYMENT:
            ep.details = {"from": self.release.software_version, "to": C.FAULTY_RELEASE.software_version,
                          "deployment_id": C.FAULTY_RELEASE.deployment_id}
            self.release = C.FAULTY_RELEASE
            self.finished.append(ep)  # instantaneous
            return
        elif scenario is Scenario.CAMERA_DEGRADATION:
            ep.details["firmware_version"] = self.release.firmware_version

        self.active[scenario] = ep

    def _end(self, scenario: Scenario, t: float) -> None:
        ep = self.active.pop(scenario)
        ep.end_s = t
        self.finished.append(ep)
        if scenario is Scenario.SENSOR_FAILURE:
            self.failed_sensor = None

    # ----------------------------------------------------------------- world

    def _spawn(self, object_type: ObjectType, x: float, y: float, vx: float, vy: float,
               episode: Optional[Episode] = None) -> WorldObject:
        self._object_seq += 1
        obj = WorldObject(f"{OBJECT_ID_PREFIX[object_type]}{self._object_seq}", object_type,
                          x, y, vx, vy, episode=episode)
        self.objects.append(obj)
        return obj

    def _spawn_background(self) -> None:
        r = self.rng
        types, weights = zip(*C.BACKGROUND_MIX)
        kind = r.choices(types, weights)[0]
        side = r.choice((-1, 1))
        x = r.uniform(30.0, C.SENSOR_RANGE)
        if kind is ObjectType.VEHICLE:
            self._spawn(kind, x, side * 3.5, self.cruise_speed + r.uniform(-4.0, 2.0), 0.0)
        elif kind is ObjectType.MOTORCYCLE:
            self._spawn(kind, x, side * 3.5, self.cruise_speed + r.uniform(-2.0, 5.0), 0.0)
        elif kind is ObjectType.CYCLIST:
            self._spawn(kind, x, side * 2.6, r.uniform(3.0, 6.0), 0.0)
        elif kind is ObjectType.PEDESTRIAN:
            self._spawn(kind, x, side * r.uniform(5.0, 7.0), r.uniform(-1.5, 1.5), 0.0)
        elif kind is ObjectType.ANIMAL:
            self._spawn(kind, x, side * r.uniform(6.0, 8.0), r.uniform(0.0, 0.5), 0.0)
        else:  # signs and cones on the roadside
            self._spawn(kind, x, side * r.uniform(4.5, 6.0), 0.0, 0.0)

    def _update_world(self, t: float, dt: float) -> None:
        for o in self.objects:
            if o.brake_at is not None and t >= o.brake_at:
                if o.resume_at is None:
                    o.vx = max(0.0, o.vx - C.LEAD_VEHICLE_DECEL * dt)
                    if o.vx == 0.0:
                        o.resume_at = t + self.rng.uniform(2.0, 4.0)
                elif t >= o.resume_at:
                    o.vx = min(o.resume_speed, o.vx + C.MAX_ACCEL * dt)
            o.x += (o.vx - self.speed) * dt
            o.y += o.vy * dt

        kept = []
        for o in self.objects:
            crossed = o.vy != 0.0 and abs(o.y) > 6.0 and o.y * o.vy > 0
            if o.x < -10.0 or o.x > C.SENSOR_RANGE + 20.0 or crossed:
                if o.episode is not None and o.episode.scenario in self.active:
                    self._end(o.episode.scenario, t)
            else:
                kept.append(o)
        self.objects = kept

        background = sum(1 for o in self.objects if o.episode is None)
        if background < C.MAX_BACKGROUND_OBJECTS and self.rng.random() < C.BACKGROUND_SPAWN_RATE * dt:
            self._spawn_background()

    # ------------------------------------------------------------ perception

    @property
    def camera_status(self) -> SensorStatus:
        if self.failed_sensor == "camera":
            return SensorStatus.FAILED
        if Scenario.CAMERA_DEGRADATION in self.active:
            return SensorStatus.DEGRADED
        return SensorStatus.OK

    @property
    def lidar_status(self) -> SensorStatus:
        return SensorStatus.FAILED if self.failed_sensor == "lidar" else SensorStatus.OK

    def _perceive(self) -> list[Detection]:
        camera, lidar = self.camera_status, self.lidar_status
        rain = Scenario.RAIN in self.active
        night = Scenario.NIGHT in self.active
        detections = []
        for o in self.objects:
            if not -5.0 < o.x <= C.SENSOR_RANGE:
                continue
            distance = math.hypot(o.x, o.y)
            distance_factor = 1.0 - 0.4 * min(distance / C.SENSOR_RANGE, 1.0)

            lidar_conf = 0.0
            if lidar is SensorStatus.OK and distance <= C.LIDAR_RANGE:
                lidar_conf = C.LIDAR_CONFIDENCE * distance_factor

            if camera is SensorStatus.FAILED:
                object_type, conf = ObjectType.UNKNOWN, lidar_conf
            else:
                object_type = o.object_type
                conf = C.BASE_CONFIDENCE[object_type] * distance_factor
                if camera is SensorStatus.DEGRADED:
                    conf *= C.DEGRADED_CAMERA_FACTOR
                if lidar is not SensorStatus.OK:
                    conf *= C.LIDAR_FAILED_FACTOR
                if rain:
                    conf *= C.RAIN_CONFIDENCE_FACTOR
                if night:
                    conf *= C.NIGHT_CONFIDENCE_FACTOR if object_type in C.VULNERABLE else C.NIGHT_CONFIDENCE_FACTOR_OTHER
                conf = max(conf, lidar_conf)

            if conf <= 0.0:
                continue
            conf = min(0.99, max(0.0, conf + self.rng.gauss(0.0, C.CONFIDENCE_NOISE)))
            if conf >= C.DETECTION_FLOOR:
                detections.append(Detection(o, object_type, conf))
        return detections

    # --------------------------------------------------------------- planner

    @staticmethod
    def _in_path(d: Detection) -> bool:
        o = d.obj
        if o.x <= 0.0:
            return False
        if abs(o.y) < C.PATH_HALF_WIDTH:
            return True
        # A pedestrian walking towards the lane is a hazard before it reaches it.
        return (d.object_type is ObjectType.PEDESTRIAN
                and abs(o.y) < C.PEDESTRIAN_HAZARD_HALF_WIDTH and o.y * o.vy < 0)

    def _plan(self, detections: list[Detection], t: float) -> Plan:
        rain = Scenario.RAIN in self.active
        target = self.cruise_speed * (C.RAIN_SPEED_FACTOR if rain else 1.0)
        if Scenario.CONSTRUCTION_ZONE in self.active:
            target = min(target, C.CONSTRUCTION_SPEED)
        if self.failed_sensor:
            target = min(target, C.SENSOR_FAILURE_SPEED)
        max_decel = C.MAX_DECEL_RAIN if rain else C.MAX_DECEL

        required_decel = 0.0
        hazard: Optional[ObjectType] = None
        for d in detections:
            if d.confidence < C.TRUST_THRESHOLD or not self._in_path(d):
                continue
            margin = C.FOLLOW_MARGIN if d.object_type is ObjectType.VEHICLE else C.STOP_MARGIN
            gap = d.obj.x - margin
            lead_speed = max(d.obj.vx, 0.0)
            safe_speed = lead_speed + math.sqrt(2 * C.COMFORT_DECEL * max(gap, 0.0))
            if safe_speed < target:
                target, hazard = safe_speed, d.object_type
            if self.speed > lead_speed:
                need = (self.speed ** 2 - lead_speed ** 2) / (2 * max(gap, 0.1))
                if need > required_decel:
                    required_decel, hazard = need, d.object_type

        emergency = required_decel > C.EMERGENCY_DECEL_THRESHOLD
        if emergency:
            accel = -max_decel
        else:
            accel = max(-C.COMFORT_DECEL, min(C.MAX_ACCEL, target - self.speed))
            if required_decel > 0.2:
                accel = min(accel, -required_decel)

        lane_changing = (self.lane_change_start is not None
                         and self.lane_change_start <= t < self.lane_change_start + C.LANE_CHANGE_DURATION)
        if emergency:
            action = PlannedAction.STOP
        elif target < 0.5 and self.speed < 0.5:
            action = PlannedAction.STOP
        elif hazard in C.VULNERABLE and accel < -0.3:
            action = PlannedAction.YIELD
        elif lane_changing:
            action = PlannedAction.LANE_CHANGE_LEFT
        elif accel > 0.3:
            action = PlannedAction.ACCELERATE
        elif accel < -0.3:
            action = PlannedAction.DECELERATE
        else:
            action = PlannedAction.CRUISE
        return Plan(action, 0.0 if emergency else max(target, 0.0), emergency, accel)

    # -------------------------------------------------------------- dynamics

    def _apply(self, plan: Plan, t: float, dt: float) -> None:
        if plan.action != self._last_action:
            if self._last_action is not None:
                self.trajectory_id = self._next_trajectory_id()
            self._last_action = plan.action

        previous = self.speed
        self.speed = max(0.0, self.speed + plan.accel * dt)
        self.accel = (self.speed - previous) / dt

        if self.lane_change_start is not None and t < self.lane_change_start + C.LANE_CHANGE_DURATION:
            phase = (t - self.lane_change_start) / C.LANE_CHANGE_DURATION
            self.steering = C.LANE_CHANGE_STEERING * math.sin(2 * math.pi * phase)
        else:
            self.steering = 0.8 * self.steering + self.rng.gauss(0.0, 1.0)
            self.heading += self.rng.gauss(0.0, 0.002)

        distance = self.speed * dt
        self.lat += distance * math.cos(self.heading) / C.METRES_PER_DEGREE
        self.lon += distance * math.sin(self.heading) / (C.METRES_PER_DEGREE * math.cos(math.radians(self.lat)))
        self.battery = max(0.0, self.battery - distance * C.BATTERY_DRAIN_PER_M)

    def _next_trajectory_id(self) -> str:
        self._trajectory_seq += 1
        return f"T{self.id}-{self._trajectory_seq}"

    # ---------------------------------------------------------------- events

    def _common(self, t: float) -> dict:
        return {
            "event_id": UUID(int=self.rng.getrandbits(128), version=4),
            "vehicle_id": self.id,
            "timestamp": self.sim_start + timedelta(milliseconds=round(t * 1000)),
        }

    def _software_event(self, t: float) -> SoftwareEvent:
        rel = self.release
        return SoftwareEvent(**self._common(t), software_version=rel.software_version,
                             perception_version=rel.perception_version,
                             firmware_version=rel.firmware_version, deployment_id=rel.deployment_id)

    def _telemetry_event(self, t: float) -> TelemetryEvent:
        return TelemetryEvent(
            **self._common(t),
            speed=round(self.speed, 2),
            acceleration=round(self.accel, 2),
            braking=round(min(1.0, max(0.0, -self.accel) / C.MAX_DECEL), 3),
            steering=round(self.steering, 1),
            battery=round(self.battery, 2),
            gps_lat=round(self.lat, 6),
            gps_lon=round(self.lon, 6),
        )

    def _perception_event(self, t: float, d: Detection) -> PerceptionEvent:
        return PerceptionEvent(
            **self._common(t),
            object_id=d.obj.object_id,
            object_type=d.object_type,
            confidence=round(d.confidence, 3),
            x=round(d.obj.x, 2),
            y=round(d.obj.y, 2),
            velocity=round(math.hypot(d.obj.vx, d.obj.vy), 2),
        )

    def _planning_event(self, t: float, plan: Plan) -> PlanningEvent:
        return PlanningEvent(
            **self._common(t),
            planned_action=plan.action,
            target_speed=round(plan.target_speed, 2),
            emergency_braking=plan.emergency_braking,
            trajectory_id=self.trajectory_id,
        )

    def _health_event(self, t: float, n_objects: int) -> HealthEvent:
        r = self.rng
        self.memory = min(90.0, max(30.0, self.memory + r.gauss(0.0, 0.2)))
        low, high = (C.DEGRADED_NETWORK_LATENCY_MS if Scenario.NETWORK_DEGRADATION in self.active
                     else C.NETWORK_LATENCY_MS)
        return HealthEvent(
            **self._common(t),
            cpu=round(min(100.0, max(0.0, 30.0 + 4.0 * n_objects + r.gauss(0.0, 3.0))), 1),
            memory=round(self.memory, 1),
            gpu=round(min(100.0, max(0.0, 40.0 + 6.0 * n_objects + r.gauss(0.0, 4.0))), 1),
            camera_status=self.camera_status,
            lidar_status=self.lidar_status,
            network_latency=round(r.uniform(low, high), 1),
        )
