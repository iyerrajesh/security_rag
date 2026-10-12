from enum import Enum
from typing import Literal

from pydantic import Field

from .base import BaseEvent


class PlannedAction(str, Enum):
    CRUISE = "cruise"
    ACCELERATE = "accelerate"
    DECELERATE = "decelerate"
    STOP = "stop"
    YIELD = "yield"
    LANE_CHANGE_LEFT = "lane_change_left"
    LANE_CHANGE_RIGHT = "lane_change_right"
    TURN_LEFT = "turn_left"
    TURN_RIGHT = "turn_right"


class PlanningEvent(BaseEvent):
    """Planner decision (topic: vehicle.planning)."""

    event_type: Literal["planning"] = "planning"

    planned_action: PlannedAction
    target_speed: float = Field(..., ge=0, description="Target speed in m/s")
    emergency_braking: bool
    trajectory_id: str = Field(..., min_length=1)
