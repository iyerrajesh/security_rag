from typing import Literal

from pydantic import Field

from .base import BaseEvent


class TelemetryEvent(BaseEvent):
    """Vehicle telemetry (topic: vehicle.telemetry)."""

    event_type: Literal["telemetry"] = "telemetry"

    speed: float = Field(..., ge=0, description="Speed in m/s")
    acceleration: float = Field(..., description="Longitudinal acceleration in m/s^2 (negative = decelerating)")
    braking: float = Field(..., ge=0, le=1, description="Brake pressure, 0 (none) to 1 (full)")
    steering: float = Field(..., ge=-540, le=540, description="Steering wheel angle in degrees (negative = left)")
    battery: float = Field(..., ge=0, le=100, description="Battery state of charge, percent")
    gps_lat: float = Field(..., ge=-90, le=90)
    gps_lon: float = Field(..., ge=-180, le=180)
