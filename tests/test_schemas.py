import io
import json
from pathlib import Path

import fastavro
import pytest

from schemas import PerceptionEvent, parse_event
from schemas.avro import AVRO_DIR, all_schemas, avro_schema, load, main, to_avro_record

EXAMPLES = sorted((Path(__file__).parent.parent / "schemas" / "examples").glob("*.json"))


def test_committed_avro_schemas_are_up_to_date():
    assert main(["--check"]) == 0, "run `python -m schemas.avro`"
    assert {p.name for p in AVRO_DIR.glob("*.avsc")} == set(all_schemas())


@pytest.mark.parametrize("filename", sorted(all_schemas()))
def test_avro_schema_parses(filename):
    fastavro.parse_schema(json.loads((AVRO_DIR / filename).read_text()))


def roundtrip(record: dict, writer: dict, reader: dict) -> dict:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(writer), record)
    buf.seek(0)
    return fastavro.schemaless_reader(buf, fastavro.parse_schema(writer), fastavro.parse_schema(reader))


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_examples_roundtrip_through_latest_avro_schema(path):
    event = parse_event(path.read_bytes())
    schema = avro_schema(type(event), type(event).latest_version())
    decoded = roundtrip(to_avro_record(event), schema, schema)
    assert type(event).model_validate(decoded) == event


def v2_event(**kw) -> PerceptionEvent:
    return PerceptionEvent(vehicle_id="V1", timestamp="2026-10-04T10:00:00Z", object_id="P1",
                           object_type="pedestrian", confidence=0.8, x=10.0, y=1.0, **kw)


def test_v1_reader_reads_v2_data():
    """Producers upgrade to v2; consumers still on v1 keep working (forward compatibility)."""
    v1, v2 = json.loads(load("perception", 1)), json.loads(load("perception", 2))
    decoded = roundtrip(to_avro_record(v2_event(occlusion=0.4)), writer=v2, reader=v1)
    assert "occlusion" not in decoded
    assert decoded["confidence"] == 0.8


def test_v2_reader_reads_v1_data():
    """Consumers upgrade first; old v1 data still reads, with occlusion defaulted (backward)."""
    v1, v2 = json.loads(load("perception", 1)), json.loads(load("perception", 2))
    record = {k: v for k, v in to_avro_record(v2_event()).items() if k != "occlusion"}
    record["schema_version"] = 1
    decoded = roundtrip(record, writer=v1, reader=v2)
    assert decoded["occlusion"] is None


def test_v1_wire_json_omits_v2_fields():
    v1 = v2_event(schema_version=1)
    assert "occlusion" not in json.loads(v1.to_wire_json())
    assert parse_event(v1.to_wire_json()) == v1
    v2 = v2_event(occlusion=0.25)
    assert json.loads(v2.to_wire_json())["occlusion"] == 0.25


def test_unknown_enum_symbol_falls_back_to_default():
    """A newer writer adding an object type doesn't break older readers."""
    v2 = json.loads(load("perception", 2))
    newer = json.loads(json.dumps(v2))
    enum = next(f for f in newer["fields"] if f["name"] == "object_type")["type"]
    enum["symbols"].append("scooter")
    record = to_avro_record(v2_event()) | {"object_type": "scooter"}
    assert roundtrip(record, writer=newer, reader=v2)["object_type"] == "unknown"
