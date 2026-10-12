"""Shared base for all fleet events.

Every event type carries `vehicle_id` + `timestamp` (the join keys used by
Kafka partitioning, Iceberg partitioning, and temporal correlation), plus
`event_id` for deduplication and `schema_version` for data contracts.
"""

from datetime import datetime, timezone
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

VEHICLE_ID_PATTERN = r"^V\d+$"
VERSION_PATTERN = r"^\d+(\.\d+){0,2}$"


class BaseEvent(BaseModel):
    # Reject unknown fields and coercion surprises: arbitrary JSON must not flow
    # through the platform (Phase 4).
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    schema_version: int = Field(1, ge=1, description="Schema version of this event type")
    event_id: UUID = Field(default_factory=uuid4, description="Unique id used for deduplication")
    vehicle_id: str = Field(..., pattern=VEHICLE_ID_PATTERN, examples=["V1023"])
    timestamp: AwareDatetime = Field(..., description="Event time (timezone-aware, normalized to UTC)")

    @field_validator("timestamp")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return v.astimezone(timezone.utc)
