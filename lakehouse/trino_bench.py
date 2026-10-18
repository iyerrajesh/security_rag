"""Trino benchmarks over the multi-day `bench` namespace (see trino_bench.sh).

    python -m lakehouse.trino_bench --days 7 --base 2026-10-01 --output results.json

Each experiment runs a query once to warm the caches, then five times, and reports the median wall time. The
amount read comes from Trino's own statistics for the query: rows and bytes read from storage, and splits (one
split is one Parquet file or part of one). Pruning shows up as those numbers falling, which is more reliable than
timing at this scale.
"""

import argparse
import json
import re
import statistics
import sys
from datetime import datetime, timedelta

from lakehouse import trino

RUNS = 5
SCAN = re.compile(r"Input: ([\d,]+) rows? \(([^)]+)\).*?Physical input: ([\d.]+\w+).*?Splits: (\d+)")


# Trino reads a Parquet file smaller than this whole, however few columns the query needs. Setting it to 0
# makes it read only the column chunks it needs, which is what a table of properly sized files does by default.
READ_ONLY_NEEDED_COLUMNS = {"lakehouse.parquet_small_file_threshold": "0B"}


def measure(sql: str, runs: int = RUNS, session: dict = None) -> dict:
    trino.run(sql, "bench", session)  # warm-up
    times, last = [], None
    for _ in range(runs):
        result = trino.run(sql, "bench", session)
        times.append(result.stats["elapsedTimeMillis"])
        last = result
    s = last.stats
    return {"median_ms": statistics.median(times), "min_ms": min(times), "rows_read": s["processedRows"],
            "bytes_read": s["physicalInputBytes"], "splits": s["totalSplits"], "result_rows": len(last.rows)}


def explain_scans(sql: str, session: dict = None) -> list[str]:
    """The table-scan lines of EXPLAIN ANALYZE (rows, bytes and files read per scan), for the write-up."""
    text = "\n".join(row[0] for row in trino.run("EXPLAIN ANALYZE " + sql, "bench", session).rows)
    return [line.strip() for line in text.splitlines() if "Physical input" in line and "Splits" in line]


def on_bench(sql: str) -> str:
    return sql.replace("curated.", "bench.")


def day_window(start: datetime, days: int = 1, column: str = '"timestamp"') -> str:
    lo, hi = start, start + timedelta(days=days)
    return (f"AND {column} >= TIMESTAMP '{lo:%Y-%m-%d %H:%M:%S} UTC' "
            f"AND {column} < TIMESTAMP '{hi:%Y-%m-%d %H:%M:%S} UTC'")


def experiments(days: int, base: datetime) -> list[tuple]:
    """(group, label, sql, note, session properties)."""
    day = base + timedelta(days=days // 2)
    one_day = day_window(day).removeprefix("AND ")
    table = "bench.vehicle_perception"
    all_columns = ("max(event_id), max(object_id), max(object_type), avg(confidence), avg(x), avg(y), avg(velocity), "
                   "avg(occlusion), max(ingest_time), max(kafka_offset)")
    hourly_raw = (f"SELECT date_trunc('hour', \"timestamp\") AS hour, object_type, count(*) AS detections, "
                  f"avg(confidence) AS avg_confidence, 100.0 * count_if(confidence < 0.5) / count(*) AS weak_pct "
                  f"FROM {table} GROUP BY 1, 2")
    hourly_pre = ("SELECT hour, object_type, sum(detections) AS detections, sum(confidence_sum) / sum(detections) AS avg_confidence, "
                  "100.0 * sum(weak_detections) / sum(detections) AS weak_pct FROM bench.perception_hourly GROUP BY 1, 2")
    failures = trino.load("pedestrian_detection_failures", "{w}")
    return [
        ("Partition pruning (day)", "all days", f"SELECT count(*), avg(confidence) FROM {table}", f"{days} days"),
        ("Partition pruning (day)", "one day", f"SELECT count(*), avg(confidence) FROM {table} WHERE {one_day}", "1 day"),
        ("Partition pruning (vehicle bucket)", "all vehicles", f"SELECT count(*), avg(confidence) FROM {table}", "100 vehicles"),
        ("Partition pruning (vehicle bucket)", "one vehicle", f"SELECT count(*), avg(confidence) FROM {table} WHERE vehicle_id = 'V1007'", "1 vehicle"),
        ("Partition pruning (both)", "one vehicle, one day", f"SELECT count(*), avg(confidence) FROM {table} WHERE vehicle_id = 'V1007' AND {one_day}", "1 vehicle, 1 day"),
        ("Column pruning (default session: files under 3 MB are read whole)", "all columns", f"SELECT {all_columns} FROM {table}", "10 columns"),
        ("Column pruning (default session: files under 3 MB are read whole)", "one column", f"SELECT avg(confidence) FROM {table}", "1 column"),
        ("Column pruning (small-file threshold 0)", "all columns", f"SELECT {all_columns} FROM {table}", "10 columns", READ_ONLY_NEEDED_COLUMNS),
        ("Column pruning (small-file threshold 0)", "one column", f"SELECT avg(confidence) FROM {table}", "1 column", READ_ONLY_NEEDED_COLUMNS),
        ("Temporal join (perception, planning, telemetry)", "all days", on_bench(failures.replace("{w}", "")), f"{days} days"),
        ("Temporal join (perception, planning, telemetry)", "one day",
         on_bench(failures.replace("{w}", day_window(day))), "1 day, filter on perception and planning"),
        ("Pre-aggregation", "detail table", hourly_raw, "every detection"),
        ("Pre-aggregation", "hourly summary", hourly_pre, "one row per vehicle, hour, type"),
    ]


def saved_query_latency(days: int) -> list[dict]:
    out = []
    for name in ("failure_rate_by_version", "pedestrian_confidence_by_firmware", "pedestrian_detection_failures", "correlated_events"):
        out.append({"query": name, **measure(on_bench(trino.load(name)))})
    return out


def format_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m lakehouse.trino_bench")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--base", default="2026-10-01", help="first simulated day, YYYY-MM-DD")
    p.add_argument("--output")
    args = p.parse_args(argv)
    base = datetime.fromisoformat(args.base)

    trino.run("DROP TABLE IF EXISTS bench.perception_hourly", "bench")
    build = trino.run(trino.load("build_perception_hourly").replace("curated.", "bench."), "bench")
    summary_rows = trino.run("SELECT count(*) FROM bench.perception_hourly", "bench").rows[0][0]
    detail_rows = trino.run("SELECT count(*) FROM bench.vehicle_perception", "bench").rows[0][0]
    print(f"bench.vehicle_perception: {detail_rows:,} rows; bench.perception_hourly: {summary_rows:,} rows "
          f"(built in {build.stats['elapsedTimeMillis'] / 1000:.1f} s)")

    results = []
    for group, label, sql, note, *session in experiments(args.days, base):
        session = session[0] if session else None
        m = measure(sql, session=session)
        m.update(group=group, label=label, note=note, scans=explain_scans(sql, session))
        results.append(m)
        print(f"  {group:68} {label:22} {m['median_ms']:>8.0f} ms  {m['rows_read']:>10,} rows  "
              f"{format_bytes(m['bytes_read']):>9}  {m['splits']:>4} splits")
    latency = saved_query_latency(args.days)
    for row in latency:
        print(f"  saved query {row['query']:44} {row['median_ms']:>8.0f} ms  {row['rows_read']:>10,} rows read")

    if args.output:
        with open(args.output, "w") as f:
            json.dump({"detail_rows": detail_rows, "summary_rows": summary_rows, "experiments": results,
                       "saved_queries": latency}, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
