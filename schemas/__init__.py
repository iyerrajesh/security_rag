"""Pydantic event schemas: single source of truth for the simulator, validator, and Iceberg tables."""

from .base import BaseEvent
from .events import TOPICS, VehicleEvent, parse_event, topic_for, vehicle_event_adapter
from .health import HealthEvent, SensorStatus
from .perception import ObjectType, PerceptionEvent
from .planning import PlannedAction, PlanningEvent
from .software import SoftwareEvent
from .telemetry import TelemetryEvent

__all__ = [
    "BaseEvent",
    "TelemetryEvent",
    "PerceptionEvent",
    "ObjectType",
    "PlanningEvent",
    "PlannedAction",
    "HealthEvent",
    "SensorStatus",
    "SoftwareEvent",
    "VehicleEvent",
    "vehicle_event_adapter",
    "parse_event",
    "topic_for",
    "TOPICS",
]
