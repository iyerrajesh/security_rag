"""Run SQL on Trino over the lakehouse.

    python -m lakehouse query                           list the saved queries
    python -m lakehouse query failure_rate_by_version   run lakehouse/queries/<name>.sql
    python -m lakehouse sql "SELECT count(*) FROM curated.vehicle_perception"

Trino's catalog is called `lakehouse`; the schemas are the Iceberg namespaces (`raw`, `validated`,
`curated`, `quarantine`), so tables are addressed as `curated.vehicle_perception`.
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import trino

QUERIES = Path(__file__).parent / "queries"
HOST = os.getenv("TRINO_HOST", "127.0.0.1")
PORT = int(os.getenv("TRINO_PORT", "8082"))


@dataclass
class Result:
    columns: list
    rows: list
    stats: dict = field(default_factory=dict)


def connect(schema: str = "curated", session: dict = None) -> trino.dbapi.Connection:
    """`session` holds session properties, e.g. {"lakehouse.parquet_small_file_threshold": "0B"}."""
    return trino.dbapi.connect(host=HOST, port=PORT, user="lakehouse", catalog="lakehouse", schema=schema,
                               session_properties=session or {})


def run(sql: str, schema: str = "curated", session: dict = None) -> Result:
    cursor = connect(schema, session).cursor()
    cursor.execute(sql.strip().rstrip(";"))
    rows = cursor.fetchall()
    return Result([c[0] for c in cursor.description or []], rows, dict(cursor.stats or {}))


def saved_queries() -> dict[str, str]:
    """{name: title} for every file in lakehouse/queries (the title is the first comment line)."""
    out = {}
    for path in sorted(QUERIES.glob("*.sql")):
        first = path.read_text().splitlines()[0]
        out[path.stem] = first.removeprefix("--").strip() if first.startswith("--") else path.stem
    return out


def load(name: str, window: str = "") -> str:
    """The SQL of a saved query. `{{and_window}}` is replaced by `window`, e.g. an `AND "timestamp" >= ...` clause."""
    path = QUERIES / f"{name}.sql"
    if not path.exists():
        raise SystemExit(f"no saved query {name!r}; choose from: {', '.join(saved_queries())}")
    return path.read_text().replace("{{and_window}}", window)


def format_table(result: Result) -> str:
    cells = [[("" if v is None else f"{v:.4g}" if isinstance(v, float) else str(v)) for v in row] for row in result.rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(result.columns)]
    lines = ["  ".join(c.ljust(w) for c, w in zip(result.columns, widths)),
             "  ".join("-" * w for w in widths)]
    lines += ["  ".join(v.rjust(w) if _numeric(v) else v.ljust(w) for v, w in zip(row, widths)) for row in cells]
    return "\n".join(lines)


def _numeric(text: str) -> bool:
    try:
        float(text)
        return True
    except ValueError:
        return False


def main(command: str, argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog=f"python -m lakehouse {command}")
    if command == "query":
        p.add_argument("name", nargs="?", help="saved query (lakehouse/queries/<name>.sql); omit to list")
    else:
        p.add_argument("sql")
    p.add_argument("--schema", default="curated", help="default schema for unqualified table names")
    p.add_argument("--json", action="store_true", help="print rows as JSON lines")
    args = p.parse_args(argv)

    if command == "query" and not args.name:
        for name, title in saved_queries().items():
            print(f"  {name:32} {title}")
        return 0
    sql = load(args.name) if command == "query" else args.sql
    result = run(sql, args.schema)
    if args.json:
        for row in result.rows:
            print(json.dumps(dict(zip(result.columns, row)), default=str))
    else:
        print(format_table(result))
        print(f"\n{len(result.rows)} rows, {result.stats.get('elapsedTimeMillis', 0) / 1000:.2f} s", file=sys.stderr)
    return 0
