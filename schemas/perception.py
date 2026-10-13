from enum import Enum
from typing import Literal, Optional

from pydantic import Field

from .base import BaseEvent, since


class ObjectType(str, Enum):
    PEDESTRIAN = "pedestrian"
    VEHICLE = "vehicle"
    CYCLIST = "cyclist"
    MOTORCYCLE = "motorcycle"
    TRAFFIC_CONE = "traffic_cone"
    TRAFFIC_SIGN = "traffic_sign"
    ANIMAL = "animal"
    UNKNOWN = "unknown"


class PerceptionEvent(BaseEvent):
    """A single detected object (topic: vehicle.perception).

    v2 (perception stack 2.1.0+) adds `occlusion`. v1 records remain valid.
    """

    schema_version: int = Field(2, ge=1, le=2, description="Schema version of this event type")
    event_type: Literal["perception"] = "perception"

    object_id: str = Field(..., min_length=1, examples=["P832"])
    object_type: ObjectType
    confidence: float = Field(..., ge=0, le=1, description="Detection confidence, 0 to 1")
    x: float = Field(..., description="Object position in the vehicle frame, metres forward")
    y: float = Field(..., description="Object position in the vehicle frame, metres left")
    velocity: Optional[float] = Field(None, description="Object speed in m/s, if tracked")
    occlusion: Optional[float] = Field(None, ge=0, le=1, json_schema_extra=since(2),
                                       description="Fraction of the object hidden from the camera, 0 to 1 (v2+)")
