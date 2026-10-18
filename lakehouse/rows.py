"""Kafka message -> table row. Pure functions, so a different writer can reuse them unchanged.

Each function takes plain values (bytes, ints, dicts), never a Kafka message object,
and returns a dict keyed by the table's column names.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID

from lakehouse.tables import MODELS


def from_millis(ms: Optional[int], fallback: datetime) -> datetime:
    """Kafka timestamp (ms since epoch) as UTC; `fallback` if the record has none."""
    if ms is None or ms < 0:
        return fallback
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _text(value: Optional[bytes]) -> Optional[str]:
    return None if value is None else value.decode("utf-8", errors="replace")


def _int(value: Optional[str]) -> Optional[int]:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def raw_row(*, key: Optional[bytes], value: Optional[bytes], partition: int, offset: int,
            timestamp_ms: Optional[int], now: datetime) -> dict:
    return {
        "timestamp": from_millis(timestamp_ms, now),
        "vehicle_id": _text(key),
        "payload": value,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "ingest_time": now,
    }


def validated_row(record: dict, event_type: str, *, partition: int, offset: int, late: bool, now: datetime) -> dict:
    """A decoded Avro record. Raises KeyError if a required column is missing."""
    row = {}
    for name, field in MODELS[event_type].model_fields.items():
        value = record.get(name)
        if isinstance(value, UUID):
            value = str(value)
        elif isinstance(value, Enum):
            value = value.value
        elif isinstance(value, datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        if value is None and field.is_required():
            raise KeyError(f"{event_type} record has no value for required field {name!r}")
        row[name] = value
    row.update(late=late, kafka_partition=partition, kafka_offset=offset, ingest_time=now)
    return row


def quarantine_row(*, key: Optional[bytes], value: Optional[bytes], headers: dict[str, bytes], partition: int,
                   offset: int, timestamp_ms: Optional[int], now: datetime) -> dict:
    header = {k: _text(v) for k, v in headers.items()}
    return {
        "timestamp": from_millis(timestamp_ms, now),
        "vehicle_id": _text(key),
        "reason": header.get("reason") or "unknown",
        "detail": header.get("detail"),
        "source_topic": header.get("source_topic"),
        "source_partition": _int(header.get("source_partition")),
        "source_offset": _int(header.get("source_offset")),
        "payload": value,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "ingest_time": now,
    }
