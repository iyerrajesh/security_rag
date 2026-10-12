import gzip
import json
from collections import Counter

import pytest

from schemas import ObjectType, parse_event
from simulator.cli import main
from simulator.engine import SimulationConfig, run
from simulator.sinks import MemorySink


def simulate(**kwargs) -> MemorySink:
    sink = MemorySink()
    run(SimulationConfig(**kwargs), sink)
    return sink


@pytest.fixture(scope="module")
def fleet() -> MemorySink:
    return simulate(vehicles=150, duration=240, seed=7)


def test_jsonl_output_validates_and_is_reproducible(tmp_path):
    for name in ("a", "b"):
        main(["--vehicles", "10", "--duration", "20", "--seed", "3", "--compress", "--output", str(tmp_path / name)])
    files = sorted(p.name for p in (tmp_path / "a").iterdir())
    assert {"telemetry.jsonl.gz", "perception.jsonl.gz", "planning.jsonl.gz", "health.jsonl.gz",
            "software.jsonl.gz", "scenarios.jsonl.gz", "manifest.json"} <= set(files)
    for f in files:
        if f != "manifest.json":  # records the output path
            assert (tmp_path / "a" / f).read_bytes() == (tmp_path / "b" / f).read_bytes(), f
    for f in files:
        if f.endswith(".jsonl.gz") and f != "scenarios.jsonl.gz":
            for line in gzip.open(tmp_path / "a" / f, "rt"):
                parse_event(line)


def test_different_seeds_differ():
    a = simulate(vehicles=5, duration=10, seed=1).events
    b = simulate(vehicles=5, duration=10, seed=2).events
    assert [e.model_dump() for e in a] != [e.model_dump() for e in b]


def test_vehicle_is_independent_of_fleet_size():
    small = [e for e in simulate(vehicles=3, duration=10).events if e.vehicle_id == "V1000"]
    large = [e for e in simulate(vehicles=20, duration=10).events if e.vehicle_id == "V1000"]
    assert [e.model_dump() for e in small] == [e.model_dump() for e in large]


def test_report_rates():
    events = simulate(vehicles=4, duration=10, events_per_second=5).events
    counts = Counter(e.event_type for e in events)
    assert counts["telemetry"] == counts["planning"] == 4 * 10 * 5
    assert counts["health"] == 4 * 10
    assert counts["software"] == 4  # boot inventory, plus any deployments
    timestamps = [e.timestamp for e in events]
    assert timestamps == sorted(timestamps)


def test_all_scenarios_occur(fleet):
    scenarios = {label["scenario"] for label in fleet.labels}
    assert scenarios == {
        "pedestrian_crossing", "hard_braking", "sensor_failure", "network_degradation", "rain",
        "night", "construction_zone", "software_deployment", "camera_degradation",
    }
    json.dumps(fleet.labels)  # serializable


def test_physical_consistency(fleet):
    tele = [e for e in fleet.events if e.event_type == "telemetry"]
    assert all(e.speed >= 0 for e in tele)
    assert min(e.acceleration for e in tele) >= -8.0
    assert max(e.acceleration for e in tele) <= 2.5
    # Emergency braking in planning shows up as hard deceleration in telemetry.
    plans = {(e.vehicle_id, e.timestamp): e for e in fleet.events if e.event_type == "planning"}
    hard = [t for t in tele if plans[(t.vehicle_id, t.timestamp)].emergency_braking]
    assert hard and all(t.acceleration < -4 or t.speed < 0.5 for t in hard)


def test_firmware_failure_chain(fleet):
    firmware, camera = {}, {}
    ped_conf = {"degraded": [], "ok": []}
    ticks, emergencies = Counter(), Counter()
    for e in fleet.events:
        if e.event_type == "software":
            firmware[e.vehicle_id] = e.firmware_version
        elif e.event_type == "health":
            camera[e.vehicle_id] = e.camera_status.value
        elif e.event_type == "perception" and e.object_type is ObjectType.PEDESTRIAN:
            if camera.get(e.vehicle_id) in ped_conf:
                ped_conf[camera[e.vehicle_id]].append(e.confidence)
        elif e.event_type == "planning":
            ticks[firmware[e.vehicle_id]] += 1
            emergencies[firmware[e.vehicle_id]] += e.emergency_braking

    def mean(xs):
        return sum(xs) / len(xs)

    # degraded camera -> lower pedestrian confidence
    assert mean(ped_conf["degraded"]) < mean(ped_conf["ok"]) - 0.1
    # firmware 3.2 -> more emergency braking
    rate = {fw: emergencies[fw] / ticks[fw] for fw in ticks}
    assert rate["3.2"] > 2 * rate["3.1"]
