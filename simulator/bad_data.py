"""Bad-data injection for testing data contracts (Phase 4).

Wraps a sink and corrupts a fraction of events on the wire. Each corrupted
record replaces its original, except `duplicate`, which sends the event twice.
Counts per kind go to the manifest so the validator's results can be checked
exactly: quarantine reasons should match the injected kinds one-for-one.

| Kind | Applies to | What is sent | Validator outcome |
|---|---|---|---|
| missing_field | all | a required payload field removed | quarantine: missing_field |
| invalid_confidence | perception | confidence outside 0..1 | quarantine: invalid_value |
| unknown_object_type | perception | object_type not in the enum | quarantine: unknown_enum |
| old_schema_version | all | schema_version 0 (never registered) | quarantine: unsupported_schema_version |
| corrupt_timestamp | all | unparseable or timezone-less timestamp | quarantine: invalid_timestamp |
| corrupt_payload | all | JSON truncated mid-record | quarantine: malformed_json |
| duplicate | all | the same event sent twice | second copy dropped as duplicate |
| late | all but software | timestamp moved 30-120 s back | accepted, counted as late |
"""

import json
import random
from collections import Counter
from datetime import timedelta

from schemas import BaseEvent

ENVELOPE = {"schema_version", "event_id", "vehicle_id", "timestamp", "event_type"}
# Must match the validator's allowed lateness so injected late events are always detected.
LATENESS = timedelta(seconds=10)

KINDS = {
    "missing_field": None,
    "invalid_confidence": {"perception"},
    "unknown_object_type": {"perception"},
    "old_schema_version": None,
    "corrupt_timestamp": None,
    "corrupt_payload": None,
    "duplicate": None,
    "late": {"telemetry", "perception", "planning", "health"},
}
CORRUPT_TIMESTAMPS = ["2026-13-45T99:00:00Z", "not-a-timestamp", "2026-10-04T10:00:00", ""]
UNKNOWN_OBJECT_TYPES = ["spaceship", "ghost", "shopping_cart"]


class BadDataSink:
    def __init__(self, inner, rate: float, seed: int):
        self.inner = inner
        self.rate = rate
        self.rng = random.Random(f"{seed}:bad-data")
        self.injected: Counter = Counter()
        # Latest timestamp of each (vehicle, type) stream that the validator will accept,
        # so `late` is only injected where it is detectable.
        self._max_valid_ts: dict = {}

    def write(self, event: BaseEvent) -> None:
        if self.rng.random() >= self.rate:
            self._write_valid(event)
            return
        kinds = [k for k, types in KINDS.items() if types is None or event.event_type in types]
        kind = self.rng.choice(kinds)
        record = json.loads(event.to_wire_json())

        if kind == "duplicate":
            self._write_valid(event)
            self._write_valid(event)
        elif kind == "late":
            stream = (event.vehicle_id, event.event_type)
            shifted = event.timestamp - timedelta(seconds=self.rng.uniform(30, 120))
            if stream not in self._max_valid_ts or shifted >= self._max_valid_ts[stream] - LATENESS:
                self._write_valid(event)  # nothing earlier to be late against: send it clean
                return
            self._raw(event, json.dumps({**record, "timestamp": shifted.isoformat().replace("+00:00", "Z")}))
        elif kind == "corrupt_payload":
            text = event.to_wire_json()
            self._raw(event, text[: self.rng.randrange(1, len(text) - 1)])
        else:
            if kind == "missing_field":
                required = [n for n, f in type(event).model_fields.items() if f.is_required() and n not in ENVELOPE]
                del record[self.rng.choice(required)]
            elif kind == "invalid_confidence":
                record["confidence"] = round(self.rng.choice([self.rng.uniform(1.01, 3), self.rng.uniform(-1, -0.01)]), 3)
            elif kind == "unknown_object_type":
                record["object_type"] = self.rng.choice(UNKNOWN_OBJECT_TYPES)
            elif kind == "old_schema_version":
                record["schema_version"] = 0
            elif kind == "corrupt_timestamp":
                record["timestamp"] = self.rng.choice(CORRUPT_TIMESTAMPS)
            self._raw(event, json.dumps(record))
        self.injected[kind] += 1

    def _write_valid(self, event: BaseEvent) -> None:
        stream = (event.vehicle_id, event.event_type)
        self._max_valid_ts[stream] = max(self._max_valid_ts.get(stream, event.timestamp), event.timestamp)
        self.inner.write(event)

    def _raw(self, event: BaseEvent, payload: str) -> None:
        self.inner.write_raw(event.event_type, event.vehicle_id, payload, event.timestamp)

    def write_label(self, record: dict) -> None:
        self.inner.write_label(record)

    def close(self) -> None:
        self.inner.close()
