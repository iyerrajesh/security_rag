from enum import Enum
from typing import Literal

from pydantic import Field

from .base import BaseEvent


class SensorStatus(str, Enum):
    OK = "ok"
    DEGRADED = "degraded"
    FAILED = "failed"
    OFFLINE = "offline"


class HealthEvent(BaseEvent):
    """Compute and sensor health (topic: vehicle.health)."""

    event_type: Literal["health"] = "health"

    cpu: float = Field(..., ge=0, le=100, description="CPU utilization, percent")
    memory: float = Field(..., ge=0, le=100, description="Memory utilization, percent")
    gpu: float = Field(..., ge=0, le=100, description="GPU utilization, percent")
    camera_status: SensorStatus
    lidar_status: SensorStatus
    network_latency: float = Field(..., ge=0, description="Network round-trip latency in ms")
