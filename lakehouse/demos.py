"""Iceberg features, shown on small scratch tables in the `demo` namespace (the real tables are untouched).

    python -m lakehouse demo evolve       schema and partition evolution without rewriting data
    python -m lakehouse demo timetravel   read the table as of an earlier snapshot or time
    python -m lakehouse demo retention    expire snapshots, then reclaim the files nobody needs
    python -m lakehouse demo all

Each demo prints what it does and returns the facts it printed, which tests/test_lakehouse.py asserts.
"""

import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, DayTransform
from pyiceberg.types import DoubleType

from lakehouse import maintenance, rows
from lakehouse.tables import MODELS, TABLE_PROPERTIES, VEHICLE_BUCKETS, validated_schema

NAMESPACE = "demo"
START = datetime(2026, 10, 4, 10, 0, 0, tzinfo=timezone.utc)


def say(text: str = "") -> None:
    print(text)


def _events(count: int, first_offset: int, with_occlusion: bool, seed: int = 7) -> list[dict]:
    rng = random.Random(seed + first_offset)
    out = []
    for i in range(count):
        record = {"schema_version": 2 if with_occlusion else 1, "event_id": uuid.UUID(int=rng.getrandbits(128)),
                  "vehicle_id": f"V{rng.randint(1, 40)}", "timestamp": START + timedelta(seconds=first_offset + i),
                  "event_type": "perception", "object_id": f"P{rng.randint(1, 999)}",
                  "object_type": rng.choice(["pedestrian", "vehicle", "cyclist", "traffic_cone"]),
                  "confidence": round(rng.random(), 3), "x": rng.uniform(0, 60), "y": rng.uniform(-8, 8),
                  "velocity": round(rng.uniform(0, 12), 2),
                  "occlusion": round(rng.random(), 3) if with_occlusion else None}
        out.append(rows.validated_row(record, "perception", partition=0, offset=first_offset + i, late=False, now=START))
    return out


def _fresh(catalog: Catalog, name: str, schema: Schema, bucket_field: str = "vehicle_id") -> Table:
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    identifier = f"{NAMESPACE}.{name}"
    try:
        catalog.purge_table(identifier)
    except NoSuchTableError:
        pass
    spec = PartitionSpec(
        PartitionField(schema.find_field("timestamp").field_id, 1000, DayTransform(), "timestamp_day"),
        PartitionField(schema.find_field(bucket_field).field_id, 1001, BucketTransform(VEHICLE_BUCKETS), "vehicle_id_bucket"),
    )
    return catalog.create_table(identifier, schema=schema, partition_spec=spec, properties=TABLE_PROPERTIES)


def _append(table: Table, batch: list[dict]) -> Table:
    columns = {f.name: [r.get(f.name) for r in batch] for f in table.schema().fields}
    table.append(pa.table(columns, schema=schema_to_pyarrow(table.schema())))
    return table


def _file_count(table: Table) -> int:
    return len(table.inspect.data_files())


def _v1_schema() -> Schema:
    """The perception table as it was before schema v2 added `occlusion`."""
    full = validated_schema(MODELS["perception"])
    return Schema(*[f for f in full.fields if f.name != "occlusion"])


def evolve(catalog: Catalog) -> dict:
    say("== Schema evolution: perception v2 adds `occlusion` ==")
    table = _fresh(catalog, "perception_evolution", _v1_schema())
    _append(table, _events(1000, 0, with_occlusion=False))
    files_v1 = _file_count(table)
    say(f"v1 table: {len(table.schema().fields)} columns, 1000 rows in {files_v1} data files")

    with table.update_schema() as update:
        update.add_column("occlusion", DoubleType(), doc="Fraction of the object hidden from the camera (v2+)")
    table = catalog.load_table(table.name())
    old = table.scan().to_arrow()
    nulls = old["occlusion"].null_count
    say(f"added `occlusion`: the {old.num_rows} existing rows read it as null ({nulls} nulls); "
        f"data files rewritten: {'no' if _file_count(table) == files_v1 else 'yes'}")

    _append(table, _events(500, 1000, with_occlusion=True))
    mixed = table.scan().to_arrow()
    say(f"after 500 v2 rows: {mixed.num_rows} rows, {mixed['occlusion'].null_count} null (v1) and "
        f"{mixed.num_rows - mixed['occlusion'].null_count} with a value (v2)")

    total_before = sum(v for v in mixed["velocity"].to_pylist() if v is not None)
    with table.update_schema() as update:
        update.rename_column("velocity", "speed_mps")
    table = catalog.load_table(table.name())
    renamed = table.scan().to_arrow()
    total_after = sum(v for v in renamed["speed_mps"].to_pylist() if v is not None)
    say(f"renamed `velocity` -> `speed_mps`: sum unchanged ({total_before:.2f} == {total_after:.2f}); "
        f"columns are tracked by id, so no file was touched")

    old_spec = table.spec().spec_id
    with table.update_spec() as update:
        update.remove_field("vehicle_id_bucket").add_field("vehicle_id", BucketTransform(16), "vehicle_id_bucket16")
    table = catalog.load_table(table.name())
    _append(table, _events(500, 1500, with_occlusion=True))
    by_spec = {}
    for spec_id in table.inspect.data_files()["spec_id"].to_pylist():
        by_spec[spec_id] = by_spec.get(spec_id, 0) + 1
    say(f"partition evolution, bucket(8) -> bucket(16): files by partition spec {dict(sorted(by_spec.items()))}; "
        f"old files keep the old layout, new data uses the new one, queries see {table.scan().to_arrow().num_rows} rows")
    return {"files_v1": files_v1, "nulls_after_add": nulls, "rows_mixed": mixed.num_rows,
            "null_occlusion_mixed": mixed["occlusion"].null_count, "velocity_sum_unchanged": abs(total_before - total_after) < 1e-9,
            "spec_ids": sorted(by_spec), "old_spec": old_spec, "rows_final": table.scan().to_arrow().num_rows}


def timetravel(catalog: Catalog) -> dict:
    say("== Time travel ==")
    table = _fresh(catalog, "perception_timetravel", validated_schema(MODELS["perception"]))
    marks = []
    for i, size in enumerate((300, 200, 100)):
        _append(table, _events(size, i * 1000, with_occlusion=True))
        table = catalog.load_table(table.name())
        marks.append((table.current_snapshot().snapshot_id, table.current_snapshot().timestamp_ms))
        time.sleep(1.1)  # distinct snapshot timestamps, so "as of a time" is unambiguous
    say(f"3 appends -> snapshots {[m[0] for m in marks]}")

    counts = {sid: table.scan(snapshot_id=sid).to_arrow().num_rows for sid, _ in marks}
    for sid, count in counts.items():
        say(f"  as of snapshot {sid}: {count} rows")

    midpoint = marks[1][1] + 500  # between the second and third commit
    snapshot = table.snapshot_as_of_timestamp(midpoint)
    as_of_time = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().num_rows
    say(f"  as of {datetime.fromtimestamp(midpoint / 1000, tz=timezone.utc):%H:%M:%S.%f} UTC: "
        f"{as_of_time} rows (snapshot {snapshot.snapshot_id})")

    before_files = {t.file.file_path for t in table.scan().plan_files()}
    result = maintenance.compact(catalog, ".".join(table.name()), min_files=2)
    table = catalog.load_table(table.name())
    after_files = {t.file.file_path for t in table.scan().plan_files()}
    now_rows = table.scan().to_arrow().num_rows
    old_rows = table.scan(snapshot_id=marks[-1][0]).to_arrow().num_rows
    say(f"compaction replaced {len(before_files)} files with {len(after_files)} "
        f"(no shared file: {not (before_files & after_files)}); current rows {now_rows}, "
        f"the pre-compaction snapshot still reads {old_rows}")
    return {"counts": list(counts.values()), "as_of_time": as_of_time, "files_before": len(before_files),
            "files_after": len(after_files), "rows_now": now_rows, "rows_old_snapshot": old_rows,
            "compacted_partitions": result.partitions}


def retention(catalog: Catalog) -> dict:
    say("== Retention: expire snapshots, then remove orphan files ==")
    table = _fresh(catalog, "perception_retention", validated_schema(MODELS["perception"]))
    for i in range(10):
        _append(table, _events(50, i * 100, with_occlusion=True))
        table = catalog.load_table(table.name())
    maintenance.compact(catalog, ".".join(table.name()), min_files=2)
    table = catalog.load_table(table.name())

    def storage() -> int:
        return len(maintenance.list_files(table))

    first = table.snapshots()[0].snapshot_id
    snapshots_before, objects_before = len(table.snapshots()), storage()
    say(f"10 appends + 1 compaction: {snapshots_before} snapshots, {_file_count(table)} live data files, "
        f"{objects_before} objects in storage")

    future = datetime.now(timezone.utc) + timedelta(minutes=1)
    expired = maintenance.expire_snapshots(table, future, retain_last=1)
    table = catalog.load_table(table.name())
    objects_expired = storage()
    try:
        table.scan(snapshot_id=first).to_arrow()
        travel = "still possible"
    except (ValueError, KeyError) as exc:
        travel = f"gone ({exc.__class__.__name__})"
    say(f"expired {len(expired)} snapshots -> {len(table.snapshots())} left; time travel to the first one: {travel}; "
        f"storage still holds {objects_expired} objects (expiry only edits metadata)")

    deleted = maintenance.remove_orphans(table, future)
    table = catalog.load_table(table.name())
    objects_after = storage()
    rows_after = table.scan().to_arrow().num_rows
    say(f"removed {len(deleted)} orphan files -> {objects_after} objects; the table still reads {rows_after} rows")
    return {"snapshots_before": snapshots_before, "expired": len(expired), "snapshots_after": len(table.snapshots()),
            "objects_before": objects_before, "objects_expired": objects_expired, "objects_after": objects_after,
            "deleted": len(deleted), "rows_after": rows_after, "time_travel": travel}


DEMOS = {"evolve": evolve, "timetravel": timetravel, "retention": retention}


def main(argv: list[str], catalog: Catalog) -> int:
    names = list(DEMOS) if argv in ([], ["all"]) else argv
    unknown = [n for n in names if n not in DEMOS]
    if unknown:
        print(f"unknown demo {unknown}; choose from {', '.join(DEMOS)} or all", file=sys.stderr)
        return 2
    for name in names:
        DEMOS[name](catalog)
        say()
    return 0
