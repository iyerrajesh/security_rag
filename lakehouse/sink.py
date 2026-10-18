"""Kafka -> Iceberg writers.

    python -m lakehouse sink raw          vehicle.*             -> raw.raw_<type>             (bytes as received)
    python -m lakehouse sink validated    validated.vehicle.*   -> validated.* / curated.*    (Avro, decoded)
    python -m lakehouse sink quarantine   quarantine.vehicle    -> quarantine.vehicle         (bytes + reason)

Each mode is its own consumer group and process, so one falling behind or failing does not stall the others.

Delivery. The table is the source of truth for progress: every commit writes the last Kafka offset
of each partition into the table's properties, in the same atomic Iceberg commit as the data. On
start, the sink seeks each partition to the offset after the one its table records, so a crash
between the Iceberg commit and the Kafka offset commit replays nothing. The Kafka group offset is
committed afterwards only so lag shows up in Kafka tooling; it is never used for resuming. The offsets
are plain table properties, readable by any engine, so another writer can take over from them.

Batching. A table is flushed when its buffer reaches --batch-rows or --commit-interval seconds pass.
Every commit makes at least one Parquet file per partition touched, so a short interval makes many
small files, which is the Phase 5 small-file experiment; compaction repairs them.
"""

import argparse
import json
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter as PromCounter
from prometheus_client import Histogram, start_http_server
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from lakehouse import config, rows
from lakehouse.tables import TableDef, tables_of
from validation import registry

OFFSET_PREFIX = "kafka.offset."
COMMIT_ATTEMPTS = 3
METRICS_PORTS = {"raw": 9109, "validated": 9110, "quarantine": 9111}

ROWS = PromCounter("lakehouse_rows_written", "Rows committed to Iceberg", ["table"])
COMMITS = PromCounter("lakehouse_commits", "Iceberg commits", ["table"])
COMMIT_SECONDS = Histogram("lakehouse_commit_seconds", "Time to build and commit one batch", ["table"])
DECODE_ERRORS = PromCounter("lakehouse_decode_errors", "Records skipped because they could not be decoded", ["topic"])
DUPLICATES = PromCounter("lakehouse_duplicate_rows", "Rows dropped as repeats of an event_id within one batch", ["table"])


def offset_property(topic: str, partition: int) -> str:
    return f"{OFFSET_PREFIX}{topic}.{partition}"


def committed_offsets(table: Table, topic: str) -> dict[int, int]:
    """{partition: last offset written} for `topic`, from the table's properties."""
    prefix = f"{OFFSET_PREFIX}{topic}."
    return {int(key[len(prefix):]): int(value) for key, value in table.properties.items() if key.startswith(prefix)}


class TableWriter:
    """Buffers rows for one table and commits them, with their Kafka offsets, in one Iceberg transaction."""

    def __init__(self, catalog: Catalog, definition: TableDef, mode: str):
        self.definition = definition
        self.catalog = catalog
        self.table = catalog.load_table(definition.identifier)
        self.arrow_schema = schema_to_pyarrow(self.table.schema())
        self.mode = mode
        self.buffer: list[dict] = []
        self.offsets: dict[int, int] = {}  # partition -> last offset seen, including records with no row
        self.since = time.monotonic()
        self.committed = 0

    def add(self, row: Optional[dict], partition: int, offset: int) -> None:
        if row is not None:
            if not self.buffer:
                self.since = time.monotonic()
            self.buffer.append(row)
        self.offsets[partition] = offset

    @property
    def pending(self) -> bool:
        return bool(self.offsets)

    def age(self) -> float:
        return time.monotonic() - self.since if self.buffer else 0.0

    def flush(self) -> int:
        """Commit the buffer. Raises if the commit fails, leaving the buffer for the caller to retry or stop."""
        if not self.pending:
            return 0
        name = self.definition.identifier
        batch = self._dedupe(self.buffer)
        with COMMIT_SECONDS.labels(name).time():
            for attempt in range(1, COMMIT_ATTEMPTS + 1):
                try:
                    self._commit(batch)
                    break
                except CommitFailedException:  # another writer committed first: refresh and retry
                    if attempt == COMMIT_ATTEMPTS:
                        raise
                    self.table = self.catalog.load_table(name)
        written = len(batch)
        ROWS.labels(name).inc(written)
        COMMITS.labels(name).inc()
        self.committed += written
        self.buffer, self.offsets, self.since = [], {}, time.monotonic()
        return written

    def _dedupe(self, batch: list[dict]) -> list[dict]:
        if "event_id" not in (batch[0] if batch else {}):
            return batch
        seen, unique = set(), []
        for row in batch:
            if row["event_id"] not in seen:
                seen.add(row["event_id"])
                unique.append(row)
        DUPLICATES.labels(self.definition.identifier).inc(len(batch) - len(unique))
        return unique

    def _commit(self, batch: list[dict]) -> None:
        topic = self.definition.topic
        properties = {offset_property(topic, p): str(o) for p, o in self.offsets.items()}
        summary = {"lakehouse.sink": self.mode, "lakehouse.offsets": json.dumps({f"{topic}/{p}": o for p, o in self.offsets.items()})}
        with self.table.transaction() as tx:
            if batch:
                tx.append(pa.Table.from_pylist(batch, schema=self.arrow_schema), snapshot_properties=summary)
            tx.set_properties(properties)


class Sink:
    def __init__(self, mode: str, args, catalog: Optional[Catalog] = None):
        from lakehouse.catalog import get_catalog

        self.mode, self.args = mode, args
        self.catalog = catalog or get_catalog()
        self.writers = {t.topic: TableWriter(self.catalog, t, mode) for t in tables_of(mode)}
        self.decoders = {}
        if mode == "validated":
            sr = registry.client(args.registry_url)
            self.decoders = {topic: AvroDeserializer(sr) for topic in self.writers}
        self.consumer = Consumer({
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id or f"lakehouse-{mode}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.totals: Counter = Counter()
        self.last_offsets: dict[tuple[str, int], int] = {}
        self.running = True

    def row_for(self, msg, writer: TableWriter, now: datetime) -> Optional[dict]:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        timestamp_ms = msg.timestamp()[1]
        if self.mode == "raw":
            return rows.raw_row(key=msg.key(), value=msg.value(), partition=partition, offset=offset,
                                timestamp_ms=timestamp_ms, now=now)
        if self.mode == "quarantine":
            headers = {k: v for k, v in (msg.headers() or [])}
            return rows.quarantine_row(key=msg.key(), value=msg.value(), headers=headers, partition=partition,
                                       offset=offset, timestamp_ms=timestamp_ms, now=now)
        headers = dict(msg.headers() or [])
        record = self.decoders[topic](msg.value(), SerializationContext(topic, MessageField.VALUE))
        return rows.validated_row(record, writer.definition.event_type, partition=partition, offset=offset,
                                  late=headers.get("late") == b"true", now=now)

    def process(self, msg) -> None:
        writer = self.writers[msg.topic()]
        try:
            row = self.row_for(msg, writer, datetime.now(timezone.utc))
        except Exception as exc:  # one undecodable record must not stop the stream; it is counted and logged
            DECODE_ERRORS.labels(msg.topic()).inc()
            self.totals["decode_errors"] += 1
            if self.totals["decode_errors"] <= 5:
                print(f"skipping {msg.topic()}[{msg.partition()}]@{msg.offset()}: {exc}", file=sys.stderr)
            row = None
        writer.add(row, msg.partition(), msg.offset())
        self.last_offsets[(msg.topic(), msg.partition())] = msg.offset()
        self.totals["consumed"] += 1

    def due(self, now: float) -> list[TableWriter]:
        return [w for w in self.writers.values()
                if len(w.buffer) >= self.args.batch_rows
                or (w.pending and now - w.since >= self.args.commit_interval)]

    def flush(self, writers: list[TableWriter]) -> None:
        for writer in writers:
            written = writer.flush()
            self.totals[f"rows:{writer.definition.identifier}"] += written
            if written:
                self.totals["commits"] += 1
        if writers:
            try:  # for lag display in Kafka tooling only: resuming uses the table, not these offsets
                self.consumer.commit(asynchronous=False)
            except Exception as exc:
                if "NO_OFFSET" not in str(exc):
                    print(f"group offset commit failed (harmless): {exc}", file=sys.stderr)

    def on_assign(self, consumer, partitions: list[TopicPartition]) -> None:
        """Start each partition just after the offset its table has committed."""
        resumed = {topic: committed_offsets(w.table, topic) for topic, w in self.writers.items()}
        for p in partitions:
            last = resumed[p.topic].get(p.partition)
            if last is not None:
                p.offset = last + 1
        consumer.assign(partitions)
        self.assigned = True
        self.last_message = time.monotonic()  # idle clock starts once we own partitions

    def run(self) -> dict:
        started = time.monotonic()
        self.last_message, self.assigned = started, False
        self.consumer.subscribe(sorted(self.writers), on_assign=self.on_assign)
        try:
            while self.running:
                now = time.monotonic()
                idle = now - self.last_message
                if self.args.idle_timeout and idle > self.args.idle_timeout and (self.assigned or idle > 60):
                    break
                self.flush(self.due(now))
                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"consumer error: {msg.error()}", file=sys.stderr)
                    continue
                self.last_message = time.monotonic()
                self.process(msg)
            self.flush([w for w in self.writers.values() if w.pending])
        finally:
            self.consumer.close()
        elapsed = max(self.last_message - started, 1e-9)
        return {"elapsed_seconds": round(elapsed, 2), "rate": round(self.totals["consumed"] / elapsed),
                **dict(sorted(self.totals.items()))}


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="python -m lakehouse sink")
    p.add_argument("mode", choices=sorted(METRICS_PORTS))
    p.add_argument("--bootstrap-servers", default=config.BOOTSTRAP_SERVERS)
    p.add_argument("--registry-url", default=config.REGISTRY_URL)
    p.add_argument("--group-id", help="consumer group (default: lakehouse-<mode>)")
    p.add_argument("--batch-rows", type=int, default=20_000, help="commit a table once it buffers this many rows")
    p.add_argument("--commit-interval", type=float, default=10.0,
                   help="commit at least this often, in seconds (a few seconds makes many small files)")
    p.add_argument("--idle-timeout", type=float, default=0.0, help="exit after this many idle seconds (0 = never)")
    p.add_argument("--metrics-port", type=int, help="Prometheus port (default per mode; 0 = disabled)")
    p.add_argument("--summary-json", help="write totals to this file on exit")
    args = p.parse_args(argv)

    port = METRICS_PORTS[args.mode] if args.metrics_port is None else args.metrics_port
    if port:
        start_http_server(port, addr="127.0.0.1")
    sink = Sink(args.mode, args)
    signal.signal(signal.SIGTERM, lambda *_: setattr(sink, "running", False))
    print(f"{args.mode}: {len(sink.writers)} tables from {', '.join(sorted(sink.writers))}", file=sys.stderr)
    try:
        summary = sink.run()
    except KeyboardInterrupt:
        sink.running = False
        return 130
    if args.summary_json:
        with open(args.summary_json, "w") as f:
            json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2), file=sys.stderr)
    return 0
