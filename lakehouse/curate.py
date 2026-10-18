"""validated -> curated: copy new validated rows, dropping any event that is already curated.

    python -m lakehouse curate            # all five event types
    python -m lakehouse curate perception

Incremental. Each run reads only validated rows whose `ingest_time` is later than the watermark stored
in the curated table's properties, and writes the watermark in the same Iceberg commit as the rows,
so a crash between the two cannot lose or repeat rows. This is correct because one writer feeds each
validated table, and it stamps `ingest_time` before the batch is committed: a later commit never
holds a row older than an earlier commit's newest row.

Repeats are found by `event_id`. A repeat is the same event reaching the validated topic twice (the
validator's dedup state is in memory), so both copies carry the same event time: the check against rows
already curated only needs to look at the time range of the new batch.
"""

import sys
import time
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, GreaterThan, GreaterThanOrEqual, LessThanOrEqual
from pyiceberg.io.pyarrow import schema_to_pyarrow

from lakehouse.tables import MODELS, TableDef, tables_of

WATERMARK = "curate.validated_ingest_watermark"


def first_of_each(event_ids: list) -> list[int]:
    """Indexes of the first occurrence of every event_id."""
    seen, keep = set(), []
    for i, event_id in enumerate(event_ids):
        if event_id not in seen:
            seen.add(event_id)
            keep.append(i)
    return keep


def curate_table(catalog: Catalog, target: TableDef, now: Optional[datetime] = None) -> dict:
    """Curate one event type. Returns {"read": n, "written": n, "repeats": n}."""
    validated = catalog.load_table(target.source)
    curated = catalog.load_table(target.identifier)
    now = now or datetime.now(timezone.utc)

    watermark = curated.properties.get(WATERMARK)
    scan = validated.scan(row_filter=GreaterThan("ingest_time", watermark)) if watermark else validated.scan()
    new = scan.to_arrow()
    read = new.num_rows
    if read == 0:
        return {"read": 0, "written": 0, "repeats": 0}
    new_watermark = pc.max(new["ingest_time"]).as_py()

    new = new.sort_by([("ingest_time", "ascending"), ("kafka_offset", "ascending")])
    new = new.take(first_of_each(new["event_id"].to_pylist()))  # repeats within the new rows

    low, high = pc.min(new["timestamp"]).as_py(), pc.max(new["timestamp"]).as_py()
    already = curated.scan(
        row_filter=And(GreaterThanOrEqual("timestamp", low.isoformat()), LessThanOrEqual("timestamp", high.isoformat())),
        selected_fields=("event_id",),
    ).to_arrow()
    if already.num_rows:
        new = new.filter(pc.invert(pc.is_in(new["event_id"], value_set=already["event_id"])))

    target_schema = schema_to_pyarrow(curated.schema())
    columns = {f.name: new[f.name] if f.name in new.column_names else pa.array([now] * new.num_rows, f.type)
               for f in target_schema}
    rows = pa.table(columns, schema=target_schema)

    with curated.transaction() as tx:
        if rows.num_rows:
            tx.append(rows, snapshot_properties={"lakehouse.sink": "curate", "lakehouse.source": target.source})
        tx.set_properties({WATERMARK: new_watermark.astimezone(timezone.utc).isoformat()})
    return {"read": read, "written": rows.num_rows, "repeats": read - rows.num_rows}


def main(argv: list[str], catalog: Catalog) -> int:
    wanted = set(argv) or set(MODELS)
    unknown = wanted - set(MODELS)
    if unknown:
        print(f"unknown event types: {', '.join(sorted(unknown))}", file=sys.stderr)
        return 2
    print(f"  {'curated table':36} {'read':>9} {'written':>9} {'repeats':>8} {'seconds':>8}")
    for target in tables_of("curated"):
        if target.event_type in wanted:
            started = time.monotonic()
            result = curate_table(catalog, target)
            print(f"  {target.identifier:36} {result['read']:>9} {result['written']:>9} {result['repeats']:>8} "
                  f"{time.monotonic() - started:>8.1f}")
    return 0
