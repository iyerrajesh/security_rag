"""Shared base for all fleet events.

Every event type carries `vehicle_id` + `timestamp` (the join keys used by
Kafka partitioning, Iceberg partitioning, and temporal correlation), plus
`event_id` for deduplication and `schema_version` for data contracts.
"""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

VEHICLE_ID_PATTERN = r"^V\d+$"
VERSION_PATTERN = r"^\d+(\.\d+){0,2}$"


def since(version: int) -> dict[str, Any]:
    """Mark a field as added in schema `version`: `Field(None, json_schema_extra=since(2))`.

    Fields added after v1 must be optional, so older records stay valid
    and the Avro schemas stay FULL-compatible.
    """
    return {"since": version}


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

    @classmethod
    def latest_version(cls) -> int:
        return cls.model_fields["schema_version"].default

    @classmethod
    def fields_for_version(cls, version: int) -> list[str]:
        """Field names that exist in schema `version` of this event type."""
        return [name for name, f in cls.model_fields.items()
                if (f.json_schema_extra or {}).get("since", 1) <= version]

    def to_wire_json(self) -> str:
        """JSON for this event's own `schema_version`: fields added later are omitted, not null."""
        keep = set(self.fields_for_version(self.schema_version))
        return self.model_dump_json(exclude={name for name in type(self).model_fields if name not in keep})
