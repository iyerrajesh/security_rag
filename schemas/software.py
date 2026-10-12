from typing import Literal

from pydantic import Field

from .base import VERSION_PATTERN, BaseEvent


class SoftwareEvent(BaseEvent):
    """Software / firmware deployment on a vehicle (topic: vehicle.software)."""

    event_type: Literal["software"] = "software"

    software_version: str = Field(..., pattern=VERSION_PATTERN, examples=["4.7"])
    perception_version: str = Field(..., pattern=VERSION_PATTERN, examples=["2.1.0"])
    firmware_version: str = Field(..., pattern=VERSION_PATTERN, examples=["3.2"])
    deployment_id: str = Field(..., min_length=1)
