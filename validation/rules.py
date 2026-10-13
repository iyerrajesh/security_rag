"""Validation rules: decide whether a raw record honours its data contract.

Pure logic, no Kafka, so it is unit-testable. Checks run in order and the
first failure is the quarantine reason:

1. malformed_json             not parseable as a JSON object
2. wrong_event_type           event_type doesn't match the topic
3. unsupported_schema_version schema_version not registered for this event type
4. missing_field / unexpected_field
                              field set doesn't match the registered Avro schema
                              for that version (the contract)
5. invalid_timestamp / unknown_enum / invalid_value / missing_field
                              semantic checks from the Pydantic model: ranges,
                              patterns, enums, timezone-aware timestamps

Stateful checks (duplicates, lateness) are in `StreamState`.
"""

import json
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pydantic import ValidationError

from schemas import TOPICS, BaseEvent

MODELS = {model.model_fields["event_type"].default: model for model in TOPICS}

REASONS = (
    "malformed_json", "wrong_event_type", "unsupported_schema_version", "missing_field",
    "unexpected_field", "invalid_timestamp", "unknown_enum", "invalid_value",
)


@dataclass(frozen=True)
class Contract:
    """The registered Avro schema for one (event type, version)."""
    event_type: str
    version: int
    fields: frozenset  # all field names
    required: frozenset  # fields without a default

    @classmethod
    def from_avro(cls, event_type: str, schema: dict) -> "Contract":
        fields = schema["fields"]
        version = next(f["default"] for f in fields if f["name"] == "schema_version")
        return cls(event_type, version, frozenset(f["name"] for f in fields),
                   frozenset(f["name"] for f in fields if "default" not in f))


@dataclass(frozen=True)
class Result:
    valid: bool
    reason: Optional[str] = None
    detail: str = ""
    event: Optional[BaseEvent] = None


def _quarantine(reason: str, detail: str) -> Result:
    return Result(False, reason, detail[:500])


def _classify(err: ValidationError) -> tuple[str, str]:
    first = err.errors()[0]
    loc = ".".join(str(part) for part in first["loc"])
    detail = f"{loc}: {first['msg']}"
    if first["loc"] and first["loc"][0] == "timestamp":
        return "invalid_timestamp", detail
    return {
        "missing": "missing_field",
        "enum": "unknown_enum",
        "extra_forbidden": "unexpected_field",
    }.get(first["type"], "invalid_value"), detail


def validate(payload: bytes, event_type: str, contracts: dict[tuple[str, int], Contract]) -> Result:
    try:
        record = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        return _quarantine("malformed_json", str(exc))
    if not isinstance(record, dict):
        return _quarantine("malformed_json", f"expected a JSON object, got {type(record).__name__}")

    if record.get("event_type") != event_type:
        return _quarantine("wrong_event_type", f"expected {event_type!r}, got {record.get('event_type')!r}")

    version = record.get("schema_version")
    contract = contracts.get((event_type, version)) if isinstance(version, int) else None
    if contract is None:
        registered = sorted(v for t, v in contracts if t == event_type)
        return _quarantine("unsupported_schema_version", f"schema_version {version!r}; registered: {registered}")

    keys = set(record)
    if missing := contract.required - keys:
        return _quarantine("missing_field", f"missing {sorted(missing)} for v{version}")
    if unexpected := keys - contract.fields:
        return _quarantine("unexpected_field", f"not in v{version}: {sorted(unexpected)}")

    try:
        event = MODELS[event_type].model_validate(record)
    except ValidationError as exc:
        return _quarantine(*_classify(exc))
    return Result(True, event=event)


class StreamState:
    """Duplicate and lateness tracking across valid events.

    Dedup is on `event_id` (perception emits several objects per timestamp, so
    `(vehicle_id, timestamp, event_type)` is not unique). State is in memory and
    bounded; after a restart, duplicates are caught downstream by idempotent writes.
    """

    def __init__(self, lateness: timedelta = timedelta(seconds=10), max_ids: int = 2_000_000):
        self.lateness = lateness
        self.max_ids = max_ids
        self._seen: OrderedDict = OrderedDict()
        self._max_ts: dict[tuple[str, str], datetime] = {}

    def is_duplicate(self, event: BaseEvent) -> bool:
        if event.event_id in self._seen:
            return True
        self._seen[event.event_id] = None
        if len(self._seen) > self.max_ids:
            self._seen.popitem(last=False)
        return False

    def is_late(self, event: BaseEvent) -> bool:
        """Late = more than `lateness` behind the newest event of the same (vehicle, type) stream."""
        stream = (event.vehicle_id, event.event_type)
        newest = self._max_ts.get(stream)
        if newest is None or event.timestamp > newest:
            self._max_ts[stream] = event.timestamp
            return False
        return event.timestamp < newest - self.lateness
