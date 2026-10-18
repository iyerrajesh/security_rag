"""Table maintenance: compaction, snapshot expiry and orphan-file removal.

    python -m lakehouse compact [TABLE ...] [--benchmark]     merge small data files
    python -m lakehouse expire [TABLE ...] --older-than 1h    drop old snapshots (metadata only)
    python -m lakehouse orphans [TABLE ...] --older-than 1h   delete files no snapshot references

Why three steps. A sink that commits every few seconds writes one Parquet file per partition per commit,
so a day of data becomes thousands of files of a few hundred KB. Planning a query has to open every
manifest and file footer, which is the cost compaction removes. But compaction only adds a new
snapshot: the old small files are still referenced by older snapshots, which is what lets you time
travel. They are freed in two steps: expire the snapshots, then delete the files nothing refers to.

Compaction is safe to run while the sinks write. It rewrites only the files it planned, and an append
that commits in between simply lands beside them.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files
from pyiceberg.table import Table

from lakehouse.tables import all_tables

SMALL_FILE_BYTES = 32 * 1024 * 1024  # a quarter of the 128 MB write target
MIN_FILES = 4  # a partition with fewer small files is left alone
COMMIT_ATTEMPTS = 3


@dataclass
class Compaction:
    table: str
    files_before: int = 0
    files_after: int = 0
    partitions: int = 0
    rows: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    snapshot_id: Optional[int] = None
    skipped: list = field(default_factory=list)


def data_file_stats(table: Table) -> dict:
    snapshot = table.current_snapshot()
    summary = snapshot.summary.additional_properties if snapshot and snapshot.summary else {}
    return {"files": int(summary.get("total-data-files", 0)), "bytes": int(summary.get("total-files-size", 0)),
            "rows": int(summary.get("total-records", 0))}


def compact(catalog: Catalog, identifier: str, small_file_bytes: int = SMALL_FILE_BYTES, min_files: int = MIN_FILES,
            max_partitions: Optional[int] = None) -> Compaction:
    """Rewrite every partition that holds at least `min_files` files under `small_file_bytes`."""
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        table = catalog.load_table(identifier)
        try:
            return _compact(catalog, table, small_file_bytes, min_files, max_partitions)
        except CommitFailedException:  # a rewrite lost a race with another rewrite: plan again
            if attempt == COMMIT_ATTEMPTS:
                raise
    raise AssertionError("unreachable")


def _compact(catalog: Catalog, table: Table, small_file_bytes: int, min_files: int, max_partitions: Optional[int]) -> Compaction:
    result = Compaction(".".join(table.name()))
    before = data_file_stats(table)
    result.files_before, result.bytes_before = before["files"], before["bytes"]
    result.files_after, result.bytes_after = before["files"], before["bytes"]

    by_partition = defaultdict(list)
    for task in table.scan().plan_files():
        if task.file.file_size_in_bytes < small_file_bytes and not task.delete_files:
            by_partition[(task.file.spec_id, tuple(task.file.partition))].append(task)
    chosen = [tasks for tasks in by_partition.values() if len(tasks) >= min_files]
    chosen.sort(key=len, reverse=True)  # the partitions with the most files gain the most
    if max_partitions:
        chosen = chosen[:max_partitions]
    if not chosen:
        return result

    scan = ArrowScan(table.metadata, table.io, table.schema(), AlwaysTrue(), case_sensitive=True)
    with table.transaction() as tx:
        with tx.update_snapshot(snapshot_properties={"lakehouse.sink": "compact"}).overwrite() as rewrite:
            for tasks in chosen:
                rows = scan.to_table(tasks)
                keys = [k for k in ("vehicle_id", "timestamp") if k in rows.column_names]
                rows = rows.sort_by([(k, "ascending") for k in keys])  # tight min/max stats for pruning
                for task in tasks:
                    rewrite.delete_data_file(task.file)
                for data_file in _dataframe_to_data_files(
                        table_metadata=tx.table_metadata, write_uuid=rewrite.commit_uuid, df=rows, io=table.io):
                    rewrite.append_data_file(data_file)
                result.rows += rows.num_rows
    result.partitions = len(chosen)

    after = catalog.load_table(".".join(table.name()))
    stats = data_file_stats(after)
    result.files_after, result.bytes_after = stats["files"], stats["bytes"]
    result.snapshot_id = after.current_snapshot().snapshot_id
    return result


def expire_snapshots(table: Table, older_than: datetime, retain_last: int = 1) -> list[int]:
    """Expire snapshots older than `older_than`, always keeping the newest `retain_last`. Returns the ids."""
    snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)
    keep = {s.snapshot_id for s in snapshots[-retain_last:]} if retain_last else set()
    cutoff_ms = int(older_than.timestamp() * 1000)
    ids = [s.snapshot_id for s in snapshots if s.timestamp_ms < cutoff_ms and s.snapshot_id not in keep]
    for snapshot_id in ids:
        # One snapshot per commit: the REST server in docker-compose (Iceberg 1.6) rejects a remove-snapshots
        # update with several ids, which is what PyIceberg sends for a batch.
        builder = table.maintenance.expire_snapshots()
        builder._snapshot_ids_to_expire = set()  # pyiceberg declares this set on the class, so it is shared
        builder.by_id(snapshot_id).commit()
    return ids


def _location(path: str) -> tuple[str, str, str]:
    """(scheme, netloc, filesystem path) for a table file URI."""
    parsed = urlparse(path)
    scheme = parsed.scheme or "file"
    return scheme, parsed.netloc, (f"{parsed.netloc}{parsed.path}" if scheme.startswith("s3") else parsed.path)


def _strip(path: str) -> str:
    """Compare locations without the scheme, so `s3://b/k`, `s3a://b/k` and `b/k` match."""
    scheme, _, fs_path = _location(path)
    return fs_path


def list_files(table: Table) -> dict[str, datetime]:
    """Every file under the table's location, as {location: last modified}."""
    from pyarrow import fs

    scheme, netloc, root = _location(table.location())
    filesystem = table.io.fs_by_scheme(scheme, netloc)
    infos = filesystem.get_file_info(fs.FileSelector(root, recursive=True))
    return {_strip(i.path if scheme.startswith("s3") else i.path): i.mtime.replace(tzinfo=timezone.utc)
            for i in infos if i.type == fs.FileType.File and i.mtime is not None}


def referenced_files(table: Table) -> set[str]:
    """Everything a remaining snapshot, or the metadata chain, still needs."""
    keep = {_strip(table.metadata_location)}
    keep.update(_strip(entry.metadata_file) for entry in table.metadata.metadata_log)
    for snapshot in table.snapshots():
        keep.add(_strip(snapshot.manifest_list))
    keep.update(_strip(p) for p in table.inspect.all_manifests()["path"].to_pylist())
    keep.update(_strip(p) for p in table.inspect.all_files()["file_path"].to_pylist())
    return keep


def remove_orphans(table: Table, older_than: datetime, dry_run: bool = False) -> list[str]:
    """Delete files under the table's location that no snapshot references. Returns the paths.

    `older_than` protects files a writer has produced but not yet committed.
    """
    keep = referenced_files(table)
    orphans = sorted(path for path, modified in list_files(table).items() if path not in keep and modified < older_than)
    if not dry_run:
        scheme, netloc, _ = _location(table.location())
        for path in orphans:
            table.io.delete(f"{scheme}://{netloc}/{path[len(netloc) + 1:]}" if scheme.startswith("s3") else path)
    return orphans


def parse_age(text: str) -> timedelta:
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return timedelta(seconds=float(text[:-1]) * units[text[-1]]) if text[-1] in units else timedelta(seconds=float(text))


def _tables(catalog: Catalog, names: list[str]) -> list[str]:
    return names or [t.identifier for t in all_tables()]


def main(command: str, argv: list[str], catalog: Catalog) -> int:
    import argparse

    p = argparse.ArgumentParser(prog=f"python -m lakehouse {command}")
    p.add_argument("tables", nargs="*", help="table identifiers (default: all)")
    if command == "compact":
        p.add_argument("--small-file-bytes", type=int, default=SMALL_FILE_BYTES)
        p.add_argument("--min-files", type=int, default=MIN_FILES)
        p.add_argument("--max-partitions", type=int)
        p.add_argument("--benchmark", action="store_true", help="time the standard queries before and after")
        p.add_argument("--output", help="with --benchmark: write the results to this JSON file")
    else:
        p.add_argument("--older-than", default="1h", help="age such as 90s, 30m, 2h, 7d (default: 1h)")
        if command == "expire":
            p.add_argument("--retain-last", type=int, default=1, help="always keep this many newest snapshots")
        else:
            p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    names = _tables(catalog, args.tables)

    if command == "compact":
        results = []
        for name in names:
            bench = None
            if args.benchmark:
                from lakehouse import benchmark
                bench = {"before": benchmark.run(catalog, name)}
            result = compact(catalog, name, args.small_file_bytes, args.min_files, args.max_partitions)
            if bench is not None:
                bench["after"] = benchmark.run(catalog, name)
                print(benchmark.report(name, result, bench["before"], bench["after"]))
            results.append({"compaction": result.__dict__, **(bench or {})})
            if bench is None and result.partitions:
                print(f"  {name:36} {result.files_before:>6} -> {result.files_after:<6} files "
                      f"({result.partitions} partitions, {result.rows} rows rewritten)")
        if args.output:
            import json
            with open(args.output, "w") as f:
                json.dump(results, f, indent=2, default=str)
        return 0

    cutoff = datetime.now(timezone.utc) - parse_age(args.older_than)
    for name in names:
        table = catalog.load_table(name)
        if command == "expire":
            ids = expire_snapshots(table, cutoff, args.retain_last)
            print(f"  {name:36} expired {len(ids)} snapshots")
        else:
            paths = remove_orphans(table, cutoff, args.dry_run)
            print(f"  {name:36} {'would delete' if args.dry_run else 'deleted'} {len(paths)} orphan files")
    return 0
