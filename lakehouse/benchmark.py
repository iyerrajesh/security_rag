"""Query timings used to show what compaction buys.

Each query runs through PyIceberg's scan (plan the files, then read them with PyArrow), so the
timings include both costs compaction changes: planning over many manifests and files, and opening
many small Parquet files. Trino arrives in Phase 6 and gets its own benchmark.

The queries follow the table. Every table gets a full scan of one column and a one-vehicle lookup. Tables with a
confidence/object_type pair (perception) also get a grouped aggregate, the shape of the Phase 6 queries.
"""

import statistics
import time
from datetime import timedelta

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

RUNS = 5  # the first run warms the file cache and is not counted


def _queries(table):
    names = {f.name for f in table.schema().fields}
    first = "timestamp"

    def vehicle() -> int:
        """One vehicle for one hour: prunes by day, by vehicle bucket, then by file statistics."""
        row = table.scan(selected_fields=("vehicle_id", first), limit=1).to_arrow()
        if row.num_rows == 0:
            return 0
        v, t = row["vehicle_id"][0].as_py(), row[first][0].as_py()
        end = t + timedelta(hours=1)
        return table.scan(
            row_filter=And(EqualTo("vehicle_id", v), And(GreaterThanOrEqual(first, t.isoformat()), LessThan(first, end.isoformat()))),
            selected_fields=("vehicle_id", first),
        ).to_arrow().num_rows

    queries = {
        "count_rows": lambda: table.scan(selected_fields=(first,)).to_arrow().num_rows,
        "one_vehicle": vehicle,
    }
    if {"object_type", "confidence"} <= names:
        def grouped() -> int:
            data = table.scan(selected_fields=("object_type", "confidence")).to_arrow()
            return data.group_by("object_type").aggregate([("confidence", "mean")]).num_rows
        queries["avg_confidence_by_type"] = grouped
    return queries


def run(catalog: Catalog, identifier: str, runs: int = RUNS) -> dict:
    table = catalog.load_table(identifier)
    started = time.perf_counter()
    files = sum(1 for _ in table.scan().plan_files())
    plan_ms = (time.perf_counter() - started) * 1000
    out = {"files": files, "plan_ms": round(plan_ms, 1), "queries": {}}
    for name, query in _queries(table).items():
        query()  # warm-up
        times = []
        for _ in range(runs):
            started = time.perf_counter()
            rows = query()
            times.append((time.perf_counter() - started) * 1000)
        out["queries"][name] = {"median_ms": round(statistics.median(times), 1), "min_ms": round(min(times), 1), "rows": rows}
    return out


def report(identifier: str, compaction, before: dict, after: dict) -> str:
    lines = [
        f"{identifier}: {compaction.files_before} -> {compaction.files_after} data files, "
        f"{compaction.partitions} partitions rewritten, {compaction.rows} rows",
        f"  {'':24} {'before (ms)':>12} {'after (ms)':>12} {'speed-up':>9}",
        f"  {'plan files':24} {before['plan_ms']:>12} {after['plan_ms']:>12} {before['plan_ms'] / max(after['plan_ms'], 0.1):>8.1f}x",
    ]
    for name, b in before["queries"].items():
        a = after["queries"][name]
        lines.append(f"  {name:24} {b['median_ms']:>12} {a['median_ms']:>12} {b['median_ms'] / max(a['median_ms'], 0.1):>8.1f}x")
    return "\n".join(lines)
