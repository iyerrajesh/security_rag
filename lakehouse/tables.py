"""Table definitions: schemas, partitioning and properties, generated from the Pydantic models.

Layers (Iceberg namespaces):

  raw         every message of `vehicle.<type>` as received: untrusted bytes + Kafka coordinates
  validated   typed events from `validated.vehicle.<type>`: passed the contract, but may hold repeats
              (the validator's dedup state is in memory, so a restart can emit an event twice)
  curated     validated events with repeats removed: what analytics queries should read
  quarantine  records the validator rejected, original bytes plus the reason

The typed schemas come from `schemas/`, so a field added to a Pydantic model shows up
here without a second definition. Iceberg tracks columns by field id, so ids are
assigned in model order and new fields must be appended, never reordered.
"""

import typing
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import AwareDatetime
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform
from pyiceberg.types import (
    BinaryType, BooleanType, DoubleType, IntegerType, LongType, NestedField, StringType, TimestamptzType,
)

from schemas import TOPICS, BaseEvent
from schemas.avro import event_type_of
from validation.service import QUARANTINE_TOPIC, validated_topic

RAW, VALIDATED, CURATED, QUARANTINE = "raw", "validated", "curated", "quarantine"
NAMESPACES = (RAW, VALIDATED, CURATED, QUARANTINE)

# Iceberg buckets by murmur3(vehicle_id) % N. N is fixed per partition spec: a micro-batch
# touches at most (days in the batch) x N partitions, so N bounds the files per commit.
# 8 keeps a 1,000-vehicle fleet at ~125 vehicles per bucket. Changing it later is a
# partition-spec evolution: old files keep the old layout.
VEHICLE_BUCKETS = 8

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
}

MODELS: dict[str, type[BaseEvent]] = {event_type_of(m): m for m in TOPICS}

VALIDATED_TABLES = {event_type: (VALIDATED, f"validated_{event_type}") for event_type in MODELS}

# Curated tables keep the analytics names from the plan; perception and planning follow the same pattern.
CURATED_TABLES = {
    "perception": (CURATED, "vehicle_perception"),
    "planning": (CURATED, "vehicle_planning"),
    "telemetry": (CURATED, "vehicle_telemetry"),
    "health": (CURATED, "vehicle_health"),
    "software": (CURATED, "software_deployments"),
}


@dataclass(frozen=True)
class TableDef:
    namespace: str
    name: str
    kind: str  # "raw" | "validated" | "curated" | "quarantine"
    topic: Optional[str]  # the Kafka topic that feeds this table (None for curated)
    schema: Schema
    event_type: Optional[str] = None
    source: Optional[str] = None  # the table a curated table is built from

    @property
    def identifier(self) -> str:
        return f"{self.namespace}.{self.name}"

    def partition_spec(self) -> PartitionSpec:
        ts, vehicle = self.schema.find_field("timestamp"), self.schema.find_field("vehicle_id")
        return PartitionSpec(
            PartitionField(ts.field_id, 1000, DayTransform(), "timestamp_day"),
            PartitionField(vehicle.field_id, 1001, BucketTransform(VEHICLE_BUCKETS), "vehicle_id_bucket"),
        ) if self.kind != QUARANTINE else PartitionSpec(
            PartitionField(ts.field_id, 1000, DayTransform(), "timestamp_day"),
        )


def _iceberg_type(annotation):
    origin = typing.get_origin(annotation)
    if origin is typing.Union:
        (inner,) = [a for a in typing.get_args(annotation) if a is not type(None)]
        return _iceberg_type(inner)
    if origin is typing.Literal:
        return StringType()
    if annotation in (AwareDatetime, datetime):
        return TimestamptzType()
    if annotation is UUID:
        return StringType()  # a plain string is readable by every engine; the id is only a dedup key
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return StringType()
    simple = {str: StringType(), int: IntegerType(), float: DoubleType(), bool: BooleanType()}
    if annotation in simple:
        return simple[annotation]
    raise TypeError(f"no Iceberg mapping for {annotation!r}")


def _is_optional(annotation) -> bool:
    return typing.get_origin(annotation) is typing.Union and type(None) in typing.get_args(annotation)


def _schema(columns: list[tuple[str, object, bool]]) -> Schema:
    return Schema(*[NestedField(i, name, typ, required=req) for i, (name, typ, req) in enumerate(columns, start=1)])


def validated_schema(model: type[BaseEvent]) -> Schema:
    """Every field of the latest model version, then where the record came from."""
    columns = [(name, _iceberg_type(f.annotation), not _is_optional(f.annotation)) for name, f in model.model_fields.items()]
    columns += [
        ("late", BooleanType(), True),  # arrived later than the validator's allowed lateness
        ("kafka_partition", IntegerType(), True),
        ("kafka_offset", LongType(), True),
        ("ingest_time", TimestamptzType(), True),
    ]
    return _schema(columns)


def curated_schema(model: type[BaseEvent]) -> Schema:
    """The validated columns plus when the row was curated. Field ids match the validated table's."""
    columns = [(f.name, f.field_type, f.required) for f in validated_schema(model).fields]
    columns.append(("curated_time", TimestamptzType(), True))
    return _schema(columns)


def raw_schema() -> Schema:
    """`timestamp` is the Kafka record timestamp, which the simulator sets to the event time."""
    return _schema([
        ("timestamp", TimestamptzType(), True),
        ("vehicle_id", StringType(), False),  # the message key; absent or wrong on bad data
        ("payload", BinaryType(), False),  # the message value, untouched
        ("kafka_partition", IntegerType(), True),
        ("kafka_offset", LongType(), True),
        ("ingest_time", TimestamptzType(), True),
    ])


def quarantine_schema() -> Schema:
    return _schema([
        ("timestamp", TimestamptzType(), True),  # Kafka record timestamp of the rejected record
        ("vehicle_id", StringType(), False),
        ("reason", StringType(), True),
        ("detail", StringType(), False),
        ("source_topic", StringType(), False),
        ("source_partition", IntegerType(), False),
        ("source_offset", LongType(), False),
        ("payload", BinaryType(), False),  # the original bytes, for investigation and replay
        ("kafka_partition", IntegerType(), True),  # position in quarantine.vehicle itself
        ("kafka_offset", LongType(), True),
        ("ingest_time", TimestamptzType(), True),
    ])


def all_tables() -> list[TableDef]:
    tables = [
        TableDef(RAW, f"raw_{event_type}", "raw", f"vehicle.{event_type}", raw_schema(), event_type)
        for event_type in MODELS
    ]
    for event_type, (namespace, name) in VALIDATED_TABLES.items():
        tables.append(TableDef(namespace, name, "validated", validated_topic(event_type),
                               validated_schema(MODELS[event_type]), event_type))
    for event_type, (namespace, name) in CURATED_TABLES.items():
        source = ".".join(VALIDATED_TABLES[event_type])
        tables.append(TableDef(namespace, name, "curated", None, curated_schema(MODELS[event_type]), event_type, source))
    tables.append(TableDef(QUARANTINE, "vehicle", "quarantine", QUARANTINE_TOPIC, quarantine_schema()))
    return tables


def tables_of(kind: str) -> list[TableDef]:
    return [t for t in all_tables() if t.kind == kind]
