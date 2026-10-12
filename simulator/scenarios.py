"""Scenario catalogue and ground-truth episode records.

Scenarios are injected per vehicle at random (rates are per vehicle-second).
Each injected episode is also written to `scenarios.jsonl` as a ground-truth
label, so later scenario mining can be scored against what really happened.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional


class Scenario(str, Enum):
    PEDESTRIAN_CROSSING = "pedestrian_crossing"
    HARD_BRAKING = "hard_braking"
    SENSOR_FAILURE = "sensor_failure"
    NETWORK_DEGRADATION = "network_degradation"
    RAIN = "rain"
    NIGHT = "night"
    CONSTRUCTION_ZONE = "construction_zone"
    SOFTWARE_DEPLOYMENT = "software_deployment"
    # Not injected directly: caused by firmware (see config.CAMERA_DEGRADE_RATE).
    CAMERA_DEGRADATION = "camera_degradation"
    # "Normal driving" is the absence of any active scenario.


@dataclass(frozen=True)
class ScenarioSpec:
    rate: float  # starts per vehicle-second
    duration: Optional[tuple[float, float]]  # None: ends when the scenario's objects leave
    initial_probability: float = 0.0  # chance the scenario is already active at t=0


SPECS = {
    Scenario.PEDESTRIAN_CROSSING: ScenarioSpec(1 / 40, None),
    Scenario.HARD_BRAKING: ScenarioSpec(1 / 120, None),
    Scenario.SENSOR_FAILURE: ScenarioSpec(1 / 600, (10.0, 40.0)),
    Scenario.NETWORK_DEGRADATION: ScenarioSpec(1 / 300, (10.0, 60.0)),
    Scenario.RAIN: ScenarioSpec(1 / 1200, (120.0, 900.0), initial_probability=0.15),
    Scenario.NIGHT: ScenarioSpec(1 / 1200, (600.0, 3600.0), initial_probability=0.2),
    Scenario.CONSTRUCTION_ZONE: ScenarioSpec(1 / 300, (20.0, 60.0)),
    Scenario.SOFTWARE_DEPLOYMENT: ScenarioSpec(1 / 120, (0.0, 0.0)),
}


@dataclass
class Episode:
    scenario: Scenario
    vehicle_id: str
    start_s: float
    end_s: float  # planned end in sim seconds; inf for object-driven scenarios
    details: dict = field(default_factory=dict)

    def to_record(self, sim_start: datetime) -> dict:
        def iso(s: float) -> str:
            return (sim_start + timedelta(milliseconds=round(s * 1000))).isoformat().replace("+00:00", "Z")

        return {
            "vehicle_id": self.vehicle_id,
            "scenario": self.scenario.value,
            "start": iso(self.start_s),
            "end": iso(self.end_s),
            "details": self.details,
        }
