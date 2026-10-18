"""Row, file and snapshot counts per table, from Iceberg metadata (no data files are read).

    python -m lakehouse status
"""

from pyiceberg.catalog import Catalog

from lakehouse.sink import committed_offsets
from lakehouse.tables import all_tables


def table_stats(catalog: Catalog) -> list[dict]:
    stats = []
    for definition in all_tables():
        table = catalog.load_table(definition.identifier)
        snapshot = table.current_snapshot()
        summary = snapshot.summary.additional_properties if snapshot and snapshot.summary else {}
        stats.append({
            "table": definition.identifier,
            "rows": int(summary.get("total-records", 0)),
            "data_files": int(summary.get("total-data-files", 0)),
            "bytes": int(summary.get("total-files-size", 0)),
            "snapshots": len(table.snapshots()),
            "offsets": committed_offsets(table, definition.topic) if definition.topic else {},
        })
    return stats


def main(catalog: Catalog) -> int:
    print(f"  {'table':40} {'rows':>10} {'files':>7} {'MB':>8} {'snapshots':>10}")
    for s in table_stats(catalog):
        print(f"  {s['table']:40} {s['rows']:>10} {s['data_files']:>7} {s['bytes'] / 1e6:>8.2f} {s['snapshots']:>10}")
    return 0
