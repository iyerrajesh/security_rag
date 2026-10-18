"""Lakehouse tests: table definitions, row mapping and the writer's commit/offset behavior.

They use a local SQL catalog with a filesystem warehouse, so no containers are needed.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from lakehouse import curate, demos, maintenance, rows
from lakehouse.bootstrap import ensure_tables
from lakehouse.catalog import get_catalog
from lakehouse.sink import TableWriter, committed_offsets, offset_property
from lakehouse.tables import MODELS, NAMESPACES, all_tables, tables_of

NOW = datetime(2026, 10, 4, 10, 30, 20, tzinfo=timezone.utc)
EVENT_TIME = datetime(2026, 10, 4, 10, 30, 15, tzinfo=timezone.utc)


@pytest.fixture
def catalog(tmp_path):
    catalog = get_catalog(f"sqlite:///{tmp_path}/catalog.db")
    ensure_tables(catalog)
    return catalog


def perception_record(**overrides) -> dict:
    record = {"schema_version": 2, "event_id": uuid.uuid4(), "vehicle_id": "V1023", "timestamp": EVENT_TIME,
              "event_type": "perception", "object_id": "P832", "object_type": "pedestrian", "confidence": 0.71,
              "x": 12.4, "y": -1.8, "velocity": None, "occlusion": 0.2}
    record.update(overrides)
    return record


def writer_for(catalog, identifier: str) -> TableWriter:
    definition = next(t for t in all_tables() if t.identifier == identifier)
    return TableWriter(catalog, definition, definition.kind)


def test_every_topic_has_a_table():
    topics = {t.topic for t in all_tables()}
    assert {f"vehicle.{t}" for t in MODELS} <= topics
    assert {f"validated.vehicle.{t}" for t in MODELS} <= topics
    assert "quarantine.vehicle" in topics
    assert {t.namespace for t in all_tables()} == set(NAMESPACES)


@pytest.mark.parametrize("event_type", sorted(MODELS))
def test_validated_schema_has_every_model_field(event_type):
    definition = next(t for t in tables_of("validated") if t.event_type == event_type)
    columns = {f.name for f in definition.schema.fields}
    assert set(MODELS[event_type].model_fields) <= columns
    assert {"late", "kafka_partition", "kafka_offset", "ingest_time"} <= columns


def test_optional_model_fields_are_optional_columns():
    schema = next(t for t in tables_of("validated") if t.event_type == "perception").schema
    assert not schema.find_field("occlusion").required  # added in v2: v1 records have none
    assert schema.find_field("confidence").required


def test_partitioned_by_day_and_vehicle_bucket():
    for definition in all_tables():
        names = [f.name for f in definition.partition_spec().fields]
        assert names[0] == "timestamp_day"
        assert (names[1:] == ["vehicle_id_bucket"]) == (definition.kind != "quarantine")


def test_bootstrap_creates_every_table_and_is_idempotent(catalog):
    assert {f"{ns}" for ns, in catalog.list_namespaces()} == set(NAMESPACES)
    assert {f"{ns}.{name}" for ns in NAMESPACES for (_, name) in catalog.list_tables(ns)} == {
        t.identifier for t in all_tables()}
    assert set(ensure_tables(catalog).values()) == {"exists"}


def test_bootstrap_reset_recreates_empty_tables(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    writer.add(rows.validated_row(perception_record(), "perception", partition=0, offset=0, late=False, now=NOW), 0, 0)
    writer.flush()
    assert writer.table.scan().to_arrow().num_rows == 1
    assert set(ensure_tables(catalog, reset=True).values()) == {"recreated"}
    assert catalog.load_table("validated.validated_perception").scan().to_arrow().num_rows == 0


def test_layers_are_built_from_the_models():
    kinds = {t.kind: [x for x in all_tables() if x.kind == t.kind] for t in all_tables()}
    assert {k: len(v) for k, v in kinds.items()} == {"raw": 5, "validated": 5, "curated": 5, "quarantine": 1}
    for table in kinds["curated"]:
        assert table.topic is None and table.source in {t.identifier for t in kinds["validated"]}


def test_validated_row_maps_types():
    row = rows.validated_row(perception_record(), "perception", partition=3, offset=42, late=True, now=NOW)
    assert isinstance(row["event_id"], str)
    assert row["object_type"] == "pedestrian"
    assert row["late"] is True and row["kafka_partition"] == 3 and row["kafka_offset"] == 42


def test_validated_row_accepts_a_v1_record_without_the_new_field():
    record = perception_record(schema_version=1)
    del record["occlusion"]
    assert rows.validated_row(record, "perception", partition=0, offset=0, late=False, now=NOW)["occlusion"] is None


def test_validated_row_rejects_a_missing_required_field():
    record = perception_record()
    del record["confidence"]
    with pytest.raises(KeyError):
        rows.validated_row(record, "perception", partition=0, offset=0, late=False, now=NOW)


def test_raw_row_keeps_bytes_untouched_and_uses_kafka_timestamp():
    payload = b'{"not": "valid\xff'
    row = rows.raw_row(key=b"V7", value=payload, partition=1, offset=9,
                       timestamp_ms=int(EVENT_TIME.timestamp() * 1000), now=NOW)
    assert row["payload"] == payload and row["vehicle_id"] == "V7" and row["timestamp"] == EVENT_TIME


def test_raw_row_without_a_kafka_timestamp_falls_back_to_ingest_time():
    assert rows.raw_row(key=None, value=b"x", partition=0, offset=0, timestamp_ms=-1, now=NOW)["timestamp"] == NOW


def test_quarantine_row_reads_the_reason_headers():
    row = rows.quarantine_row(
        key=b"V7", value=b"{}", partition=2, offset=5, timestamp_ms=None, now=NOW,
        headers={"reason": b"invalid_value", "detail": b"confidence 1.4", "source_topic": b"vehicle.perception",
                 "source_partition": b"4", "source_offset": b"1001"})
    assert (row["reason"], row["source_topic"], row["source_partition"], row["source_offset"]) == (
        "invalid_value", "vehicle.perception", 4, 1001)


def test_quarantine_row_survives_missing_headers():
    row = rows.quarantine_row(key=None, value=b"?", headers={}, partition=0, offset=0, timestamp_ms=None, now=NOW)
    assert row["reason"] == "unknown" and row["source_offset"] is None


def test_flush_writes_rows_and_records_offsets_in_the_same_commit(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    for offset in range(5):
        row = rows.validated_row(perception_record(), "perception", partition=offset % 2, offset=offset, late=False, now=NOW)
        writer.add(row, offset % 2, offset)
    assert writer.flush() == 5

    table = catalog.load_table("validated.validated_perception")
    assert table.scan().to_arrow().num_rows == 5
    assert committed_offsets(table, "validated.vehicle.perception") == {0: 4, 1: 3}
    assert table.current_snapshot().summary.additional_properties["lakehouse.sink"] == "validated"

    writer.add(rows.validated_row(perception_record(), "perception", partition=0, offset=5, late=False, now=NOW), 0, 5)
    writer.flush()
    table = catalog.load_table("validated.validated_perception")
    assert committed_offsets(table, "validated.vehicle.perception")[0] == 5  # advances, keeps other partitions
    assert committed_offsets(table, "validated.vehicle.perception")[1] == 3
    assert len(table.snapshots()) == 2


def test_offsets_survive_in_table_properties_not_only_snapshots(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    writer.add(rows.validated_row(perception_record(), "perception", partition=0, offset=7, late=False, now=NOW), 0, 7)
    writer.flush()
    properties = catalog.load_table("validated.validated_perception").properties
    assert properties[offset_property("validated.vehicle.perception", 0)] == "7"


def test_records_skipped_as_undecodable_still_advance_the_offset(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    writer.add(None, 0, 11)  # a record that could not be decoded: no row, but progress is recorded
    assert writer.flush() == 0
    table = catalog.load_table("validated.validated_perception")
    assert table.scan().to_arrow().num_rows == 0
    assert committed_offsets(table, "validated.vehicle.perception") == {0: 11}


def test_a_repeated_event_id_in_one_batch_is_written_once(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    event_id = uuid.uuid4()
    for offset in range(3):
        row = rows.validated_row(perception_record(event_id=event_id), "perception", partition=0, offset=offset,
                                 late=False, now=NOW)
        writer.add(row, 0, offset)
    assert writer.flush() == 1


def test_raw_and_quarantine_tables_accept_rows(catalog):
    raw = writer_for(catalog, "raw.raw_perception")
    raw.add(rows.raw_row(key=None, value=b"\xff\xfe", partition=0, offset=0, timestamp_ms=None, now=NOW), 0, 0)
    assert raw.flush() == 1  # no vehicle_id and non-UTF-8 bytes still land

    quarantine = writer_for(catalog, "quarantine.vehicle")
    quarantine.add(rows.quarantine_row(key=b"V1", value=b"{", headers={"reason": b"malformed_json"}, partition=0,
                                       offset=0, timestamp_ms=None, now=NOW), 0, 0)
    assert quarantine.flush() == 1
    assert catalog.load_table("quarantine.vehicle").scan().to_arrow().to_pylist()[0]["reason"] == "malformed_json"


def test_rows_are_spread_over_vehicle_buckets(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    for i in range(40):
        row = rows.validated_row(perception_record(vehicle_id=f"V{i}"), "perception", partition=0, offset=i, late=False, now=NOW)
        writer.add(row, 0, i)
    writer.flush()
    files = catalog.load_table("validated.validated_perception").inspect.files().to_pylist()
    assert 1 < len(files) <= 8  # one day, up to 8 buckets


def land(catalog, identifier: str, records: list[dict], first_offset: int = 0, partition: int = 0) -> None:
    """Write validated rows to a validated table, one commit per call."""
    writer = writer_for(catalog, identifier)
    for i, record in enumerate(records):
        offset = first_offset + i
        writer.add(rows.validated_row(record, "perception", partition=partition, offset=offset, late=False,
                                      now=NOW + timedelta(seconds=offset)), partition, offset)
    writer.flush()


def curated_ids(catalog) -> list[str]:
    return catalog.load_table("curated.vehicle_perception").scan(selected_fields=("event_id",)).to_arrow()["event_id"].to_pylist()


def curate_perception(catalog) -> dict:
    target = next(t for t in tables_of("curated") if t.event_type == "perception")
    return curate.curate_table(catalog, target, now=NOW)


def test_curate_copies_new_rows_and_adds_curated_time(catalog):
    land(catalog, "validated.validated_perception", [perception_record() for _ in range(5)])
    assert curate_perception(catalog) == {"read": 5, "written": 5, "repeats": 0}
    out = catalog.load_table("curated.vehicle_perception").scan().to_arrow().to_pylist()
    assert len(out) == 5 and all(r["curated_time"] == NOW for r in out)


def test_curate_is_incremental(catalog):
    land(catalog, "validated.validated_perception", [perception_record() for _ in range(3)])
    curate_perception(catalog)
    assert curate_perception(catalog) == {"read": 0, "written": 0, "repeats": 0}  # nothing new
    land(catalog, "validated.validated_perception", [perception_record() for _ in range(2)], first_offset=3)
    assert curate_perception(catalog) == {"read": 2, "written": 2, "repeats": 0}  # only the new rows
    assert len(curated_ids(catalog)) == 5


def test_curate_drops_a_repeat_within_a_batch_and_across_runs(catalog):
    event_id = uuid.uuid4()
    # the sink dedupes within a commit, so a repeat reaches the table as a second commit
    land(catalog, "validated.validated_perception", [perception_record(event_id=event_id), perception_record()])
    land(catalog, "validated.validated_perception", [perception_record(event_id=event_id)], first_offset=5)
    assert curate_perception(catalog) == {"read": 3, "written": 2, "repeats": 1}
    # the validator re-emits the same event later (its dedup state was lost): it must not be curated again
    land(catalog, "validated.validated_perception", [perception_record(event_id=event_id)], first_offset=10)
    assert curate_perception(catalog) == {"read": 1, "written": 0, "repeats": 1}
    ids = curated_ids(catalog)
    assert len(ids) == len(set(ids)) == 2


def test_curate_keeps_the_first_copy(catalog):
    event_id = uuid.uuid4()
    land(catalog, "validated.validated_perception", [perception_record(event_id=event_id, confidence=0.1)])
    land(catalog, "validated.validated_perception", [perception_record(event_id=event_id, confidence=0.9)], first_offset=1)
    curate_perception(catalog)
    (row,) = catalog.load_table("curated.vehicle_perception").scan().to_arrow().to_pylist()
    assert row["confidence"] == 0.1 and row["kafka_offset"] == 0


def small_file_table(catalog, commits: int = 6, per_commit: int = 40):
    for c in range(commits):
        land(catalog, "validated.validated_perception",
             [perception_record(vehicle_id=f"V{i}") for i in range(per_commit)], first_offset=c * per_commit)
    return catalog.load_table("validated.validated_perception")


def test_compaction_merges_small_files_without_changing_the_data(catalog):
    before = small_file_table(catalog)
    files_before = len(before.inspect.data_files())
    ids_before = sorted(before.scan(selected_fields=("event_id",)).to_arrow()["event_id"].to_pylist())

    result = maintenance.compact(catalog, "validated.validated_perception", min_files=2)

    after = catalog.load_table("validated.validated_perception")
    assert result.files_before == files_before and result.files_after == len(after.inspect.data_files())
    assert result.files_after < files_before / 2
    assert sorted(after.scan(selected_fields=("event_id",)).to_arrow()["event_id"].to_pylist()) == ids_before
    assert after.current_snapshot().summary.additional_properties["lakehouse.sink"] == "compact"


def test_compaction_keeps_the_kafka_offsets(catalog):
    small_file_table(catalog)
    before = committed_offsets(catalog.load_table("validated.validated_perception"), "validated.vehicle.perception")
    maintenance.compact(catalog, "validated.validated_perception", min_files=2)
    assert committed_offsets(catalog.load_table("validated.validated_perception"), "validated.vehicle.perception") == before


def test_compaction_leaves_a_partition_with_too_few_files_alone(catalog):
    small_file_table(catalog, commits=2, per_commit=3)
    result = maintenance.compact(catalog, "validated.validated_perception", min_files=50)
    assert result.partitions == 0 and result.files_after == result.files_before


def test_a_sink_can_keep_writing_after_a_compaction(catalog):
    writer = writer_for(catalog, "validated.validated_perception")
    small_file_table(catalog)
    maintenance.compact(catalog, "validated.validated_perception", min_files=2)
    # this writer loaded its table before the compaction: its next commit must still land
    writer.add(rows.validated_row(perception_record(), "perception", partition=0, offset=999, late=False, now=NOW), 0, 999)
    writer.flush()
    assert catalog.load_table("validated.validated_perception").scan().to_arrow().num_rows == 241


def test_compacting_an_empty_table_is_a_no_op(catalog):
    assert maintenance.compact(catalog, "validated.validated_perception").partitions == 0


def test_expire_keeps_the_newest_snapshot_and_time_travel_ends(catalog):
    table = small_file_table(catalog, commits=4)
    first = table.snapshots()[0].snapshot_id
    expired = maintenance.expire_snapshots(table, datetime.now(timezone.utc) + timedelta(minutes=1), retain_last=1)
    table = catalog.load_table("validated.validated_perception")
    assert len(expired) == 3 and len(table.snapshots()) == 1
    assert table.scan().to_arrow().num_rows == 160
    with pytest.raises((ValueError, KeyError)):
        table.scan(snapshot_id=first).to_arrow()


def test_orphans_are_only_removed_after_their_snapshots_expire(catalog):
    small_file_table(catalog)
    maintenance.compact(catalog, "validated.validated_perception", min_files=2)
    table = catalog.load_table("validated.validated_perception")
    future = datetime.now(timezone.utc) + timedelta(minutes=1)
    assert maintenance.remove_orphans(table, future, dry_run=True) == []  # old snapshots still reference everything

    maintenance.expire_snapshots(table, future, retain_last=1)
    table = catalog.load_table("validated.validated_perception")
    orphans = maintenance.remove_orphans(table, future)
    assert orphans and table.scan().to_arrow().num_rows == 240
    assert maintenance.remove_orphans(table, future) == []
    assert not any(maintenance._strip(p) in set(orphans) for p in table.inspect.all_files()["file_path"].to_pylist())


def test_orphans_younger_than_the_cutoff_are_kept(catalog):
    small_file_table(catalog)
    maintenance.compact(catalog, "validated.validated_perception", min_files=2)
    table = catalog.load_table("validated.validated_perception")
    maintenance.expire_snapshots(table, datetime.now(timezone.utc) + timedelta(minutes=1), retain_last=1)
    table = catalog.load_table("validated.validated_perception")
    assert maintenance.remove_orphans(table, datetime.now(timezone.utc) - timedelta(hours=1)) == []  # a writer may still commit them


def test_parse_age():
    assert maintenance.parse_age("90s") == timedelta(seconds=90) and maintenance.parse_age("2h") == timedelta(hours=2)
    assert maintenance.parse_age("0s") == timedelta(0) and maintenance.parse_age("30") == timedelta(seconds=30)


def test_demo_schema_evolution(catalog):
    facts = demos.evolve(catalog)
    assert facts["nulls_after_add"] == 1000 and facts["null_occlusion_mixed"] == 1000
    assert facts["velocity_sum_unchanged"] and facts["rows_final"] == 2000 and len(facts["spec_ids"]) == 2


def test_demo_time_travel(catalog):
    facts = demos.timetravel(catalog)
    assert facts["counts"] == [300, 500, 600] and facts["as_of_time"] == 500
    assert facts["rows_now"] == facts["rows_old_snapshot"] == 600 and facts["files_after"] < facts["files_before"]


def test_demo_retention(catalog):
    facts = demos.retention(catalog)
    assert facts["snapshots_before"] == 11 and facts["snapshots_after"] == 1 and facts["rows_after"] == 500
    assert facts["objects_expired"] >= facts["objects_before"] and facts["objects_after"] < facts["objects_before"] / 2
    assert facts["time_travel"].startswith("gone")
