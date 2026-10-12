"""Union of all event types, discriminated by `event_type`, plus topic routing."""

from typing import Annotated, Union

from pydantic import Field, TypeAdapter

from .base import BaseEvent
from .health import HealthEvent
from .perception import PerceptionEvent
from .planning import PlanningEvent
from .software import SoftwareEvent
from .telemetry import TelemetryEvent

VehicleEvent = Annotated[
    Union[TelemetryEvent, PerceptionEvent, PlanningEvent, HealthEvent, SoftwareEvent],
    Field(discriminator="event_type"),
]

vehicle_event_adapter: TypeAdapter[VehicleEvent] = TypeAdapter(VehicleEvent)

TOPICS: dict[type[BaseEvent], str] = {
    TelemetryEvent: "vehicle.telemetry",
    PerceptionEvent: "vehicle.perception",
    PlanningEvent: "vehicle.planning",
    HealthEvent: "vehicle.health",
    SoftwareEvent: "vehicle.software",
}


def parse_event(data: Union[dict, str, bytes]) -> VehicleEvent:
    """Validate a raw event (dict or JSON) into its concrete model; raises ValidationError."""
    if isinstance(data, dict):
        return vehicle_event_adapter.validate_python(data)
    return vehicle_event_adapter.validate_json(data)


def topic_for(event: BaseEvent) -> str:
    return TOPICS[type(event)]
