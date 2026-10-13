import json
from collections import Counter

import pytest

from schemas.avro import all_schemas
from simulator.bad_data import BadDataSink
from simulator.engine import SimulationConfig, run
from simulator.sinks import MemorySink
from validation.rules import Contract, StreamState, validate

CONTRACTS = {}
for filename, schema in all_schemas().items():
    contract = Contract.from_avro(filename.split(".")[0], schema)
    CONTRACTS[(contract.event_type, contract.version)] = contract

EXPECTED_REASON = {
    "missing_field": "missing_field",
    "invalid_confidence": "invalid_value",
    "unknown_object_type": "unknown_enum",
    "old_schema_version": "unsupported_schema_version",
    "corrupt_timestamp": "invalid_timestamp",
    "corrupt_payload": "malformed_json",
}


def perception(**overrides) -> bytes:
    record = {"event_type": "perception", "schema_version": 2, "event_id": "3f1c2a9e-8b4d-4e2f-9a61-0c7d5e8b1a02",
              "vehicle_id": "V1023", "timestamp": "2026-10-04T10:30:15Z", "object_id": "P832",
              "object_type": "pedestrian", "confidence": 0.71, "x": 12.4, "y": -1.8, "velocity": 1.3,
              "occlusion": 0.2}
    record.update(overrides)
    return json.dumps({k: v for k, v in record.items() if v is not ...}).encode()


@pytest.mark.parametrize("payload, reason", [
    (b'{"event_type": "percep', "malformed_json"),
    (b"[1, 2]", "malformed_json"),
    (perception(event_type="planning"), "wrong_event_type"),
    (perception(schema_version=0), "unsupported_schema_version"),
    (perception(schema_version="2"), "unsupported_schema_version"),
    (perception(confidence=...), "missing_field"),
    (perception(schema_version=1), "unexpected_field"),  # occlusion doesn't exist in v1
    (perception(colour="red"), "unexpected_field"),
    (perception(timestamp="2026-10-04T10:30:15"), "invalid_timestamp"),
    (perception(timestamp="garbage"), "invalid_timestamp"),
    (perception(object_type="spaceship"), "unknown_enum"),
    (perception(confidence=1.5), "invalid_value"),
    (perception(vehicle_id="car-7"), "invalid_value"),
])
def test_quarantine_reasons(payload, reason):
    result = validate(payload, "perception", CONTRACTS)
    assert not result.valid and result.reason == reason, result


def test_v1_and_v2_are_both_valid():
    assert validate(perception(), "perception", CONTRACTS).valid
    assert validate(perception(schema_version=1, occlusion=...), "perception", CONTRACTS).valid


def test_injected_bad_data_maps_one_to_one_to_reasons():
    """Every injected corruption is caught with the expected reason; nothing clean is rejected."""
    inner = MemorySink()
    bad = BadDataSink(inner, rate=0.2, seed=11)
    run(SimulationConfig(vehicles=30, duration=60, seed=11), bad)

    state, reasons, duplicates, late = StreamState(), Counter(), 0, 0
    for event_type, payload in inner.raw:  # corrupted records, written as raw payloads
        result = validate(payload.encode(), event_type, CONTRACTS)
        if result.valid:
            late += 1  # the only valid raw records are `late` ones
        else:
            reasons[result.reason] += 1
    for event in inner.events:  # clean records, including both copies of duplicates
        result = validate(event.to_wire_json().encode(), event.event_type, CONTRACTS)
        assert result.valid, result
        duplicates += state.is_duplicate(result.event)

    expected = Counter()
    for kind, n in bad.injected.items():
        if kind in EXPECTED_REASON:
            expected[EXPECTED_REASON[kind]] += n
    assert reasons == expected
    assert duplicates == bad.injected["duplicate"]
    assert late == bad.injected["late"]


def test_lateness():
    from schemas import parse_event

    state = StreamState()

    def event(ts: str):
        return parse_event(perception(timestamp=ts, event_id=...))  # fresh event_id each time

    assert not state.is_late(event("2026-10-04T10:00:30Z"))
    assert not state.is_late(event("2026-10-04T10:00:25Z"))  # 5 s behind: within allowed lateness
    assert state.is_late(event("2026-10-04T10:00:10Z"))      # 20 s behind
