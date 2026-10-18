"""Tests for the Trino side that need no running Trino: saved queries, the benchmark definitions, the backfill loader."""

import re
import subprocess
import sys
from datetime import datetime

import pytest

from lakehouse import backfill, trino, trino_bench
from lakehouse.bootstrap import ensure_tables
from lakehouse.catalog import get_catalog
from lakehouse.tables import CURATED_TABLES, tables_of

KNOWN_TABLES = {f"{ns}.{name}" for ns, name in CURATED_TABLES.values()} | {"quarantine.vehicle", "curated.perception_hourly"}


def test_every_saved_query_has_a_title_and_names_only_known_tables():
    queries = trino.saved_queries()
    assert {"failure_rate_by_version", "pedestrian_detection_failures", "correlated_events"} <= set(queries)
    for name, title in queries.items():
        sql = trino.load(name)
        assert sql.startswith("--") and title and title != name, name
        referenced = set(re.findall(r"\b(?:curated|quarantine|validated|raw)\.\w+", re.sub(r"--.*", "", sql)))
        assert referenced <= KNOWN_TABLES, f"{name} refers to {referenced - KNOWN_TABLES}"


def test_the_time_window_placeholder_is_filled_or_emptied():
    assert "{{" not in trino.load("pedestrian_detection_failures")
    assert 'AND "timestamp" >= X' in trino.load("pedestrian_detection_failures", 'AND "timestamp" >= X')


def test_an_unknown_query_name_lists_the_choices():
    with pytest.raises(SystemExit) as error:
        trino.load("nope")
    assert "failure_rate_by_version" in str(error.value)


def test_format_table_aligns_and_rounds():
    out = trino.format_table(trino.Result(["name", "rate"], [["a", 0.123456], ["bb", None]]))
    lines = out.splitlines()
    assert lines[0].split() == ["name", "rate"] and "0.1235" in lines[2] and len(lines) == 4


def test_benchmark_queries_target_the_bench_namespace_and_have_no_loose_placeholders():
    experiments = trino_bench.experiments(7, datetime(2026, 10, 1))
    groups = {e[0].split(" (")[0] for e in experiments}
    assert {"Partition pruning", "Column pruning", "Temporal join", "Pre-aggregation"} <= groups
    for group, label, sql, *_ in experiments:
        assert "bench." in sql and "curated." not in sql and "{" not in sql, (group, label)


def test_day_window_is_half_open_utc():
    window = trino_bench.day_window(datetime(2026, 10, 4))
    assert "'2026-10-04 00:00:00 UTC'" in window and "< TIMESTAMP '2026-10-05 00:00:00 UTC'" in window


def test_format_bytes():
    assert trino_bench.format_bytes(512) == "512 B" and trino_bench.format_bytes(66 * 1024 * 1024) == "66.0 MB"


@pytest.fixture
def simulated(tmp_path):
    out = tmp_path / "day"
    subprocess.run([sys.executable, "-m", "simulator", "--vehicles", "4", "--duration", "30", "--seed", "3",
                    "--start-time", "2026-10-03T00:00:00Z", "--output", str(out)], check=True, capture_output=True)
    return out


def test_backfill_loads_a_simulator_run_into_the_bench_namespace(tmp_path, simulated):
    catalog = get_catalog(f"sqlite:///{tmp_path}/catalog.db")
    tables = backfill.bench_tables("bench")
    ensure_tables(catalog, tables)
    counts = backfill.load_directory(catalog, simulated, tables)

    for table in tables:
        lines = sum(1 for _ in open(simulated / f"{table.event_type}.jsonl"))
        assert counts[table.event_type] == lines
        scan = catalog.load_table(table.identifier).scan().to_arrow()
        assert scan.num_rows == lines
        assert scan["timestamp"][0].as_py().date().isoformat() == "2026-10-03"
        ids = scan["event_id"].to_pylist()
        assert len(ids) == len(set(ids))
    assert {t.namespace for t in tables} == {"bench"} and all(t.kind == "curated" for t in tables)
    assert "bench" in {ns[0] for ns in catalog.list_namespaces()}


def test_backfill_leaves_the_real_tables_alone(tmp_path, simulated):
    catalog = get_catalog(f"sqlite:///{tmp_path}/catalog.db")
    ensure_tables(catalog)
    tables = backfill.bench_tables("bench")
    ensure_tables(catalog, tables)
    backfill.load_directory(catalog, simulated, tables)
    for table in tables_of("curated"):
        assert catalog.load_table(table.identifier).scan().to_arrow().num_rows == 0


def test_backfill_rejects_a_line_that_breaks_the_contract(tmp_path, simulated):
    with open(simulated / "telemetry.jsonl", "a") as f:
        f.write('{"event_type": "telemetry", "vehicle_id": "V1"}\n')
    catalog = get_catalog(f"sqlite:///{tmp_path}/catalog.db")
    tables = backfill.bench_tables("bench")
    ensure_tables(catalog, tables)
    with pytest.raises(Exception):
        backfill.load_directory(catalog, simulated, tables)
