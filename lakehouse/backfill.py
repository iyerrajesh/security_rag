"""Load simulator JSONL output straight into curated-shaped tables in another namespace.

    python -m lakehouse backfill output/bench/day0 output/bench/day1 ... --namespace bench [--reset]

For benchmarks that need more than one day of data. The live pipeline cannot supply it: Kafka retention counts
from event time, so records dated days ago are deleted as they arrive (see ingestion/README.md). This loader
skips Kafka and the validator, but still parses every line with the Pydantic models, so a bad line stops the load.
The tables have the curated schema and layout (same columns, `day(timestamp)` + `bucket(8, vehicle_id)`), in
namespace `bench` by default, so the real tables are untouched.
"""

import argparse
import dataclasses
import gzip
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from lakehouse import rows
from lakehouse.bootstrap import ensure_tables
from lakehouse.tables import MODELS, TableDef, tables_of
from schemas import parse_event

BATCH_ROWS = 500_000


def bench_tables(namespace: str) -> list[TableDef]:
    return [dataclasses.replace(t, namespace=namespace) for t in tables_of("curated")]


def load_directory(catalog: Catalog, directory: Path, tables: list[TableDef]) -> dict[str, int]:
    """Append one simulator run (a directory of `<type>.jsonl[.gz]` files). Returns {event_type: rows}."""
    counts = {}
    now = datetime.now(timezone.utc)
    for table in tables:
        path = next((p for p in (directory / f"{table.event_type}.jsonl", directory / f"{table.event_type}.jsonl.gz") if p.exists()), None)
        if path is None:
            continue
        iceberg = catalog.load_table(table.identifier)
        arrow_schema = schema_to_pyarrow(iceberg.schema())
        batch, total = [], 0
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as lines:
            for offset, line in enumerate(lines):
                event = parse_event(line)
                row = rows.validated_row(event.model_dump(), table.event_type, partition=0, offset=offset, late=False, now=now)
                row["curated_time"] = now
                batch.append(row)
                if len(batch) >= BATCH_ROWS:
                    total += _flush(iceberg, arrow_schema, batch)
                    batch = []
        total += _flush(iceberg, arrow_schema, batch)
        counts[table.event_type] = total
    return counts


def _flush(iceberg, arrow_schema, batch: list[dict]) -> int:
    if not batch:
        return 0
    columns = {f.name: [r[f.name] for r in batch] for f in arrow_schema}
    iceberg.append(pa.table(columns, schema=arrow_schema).sort_by([("vehicle_id", "ascending"), ("timestamp", "ascending")]))
    return len(batch)


def main(argv: list[str], catalog: Catalog) -> int:
    p = argparse.ArgumentParser(prog="python -m lakehouse backfill")
    p.add_argument("directories", nargs="+", type=Path, help="simulator output directories (one per day)")
    p.add_argument("--namespace", default="bench")
    p.add_argument("--reset", action="store_true", help="drop and recreate the tables first")
    args = p.parse_args(argv)

    tables = bench_tables(args.namespace)
    ensure_tables(catalog, tables, reset=args.reset)
    for directory in args.directories:
        started = time.monotonic()
        counts = load_directory(catalog, directory, tables)
        print(f"  {directory}: {sum(counts.values()):>9} rows in {time.monotonic() - started:5.1f} s "
              f"({', '.join(f'{k} {v}' for k, v in counts.items())})")
    return 0
