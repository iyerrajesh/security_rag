# Lakehouse (Phase 5)

Kafka -> Iceberg tables on MinIO, through an Iceberg REST catalog. Writers, layers, compaction,
snapshot retention and the Iceberg feature demos are in place. Trino (Phase 6) queries the tables: see
[`query-design.md`](query-design.md).

```
docker compose --profile m1 up -d --wait kafka kafka-init schema-registry minio iceberg-rest
.venv/bin/python -m lakehouse bootstrap                 # namespaces + tables (idempotent; --reset drops them)
.venv/bin/python -m lakehouse sink raw &                # vehicle.*             -> raw.raw_<type>
.venv/bin/python -m lakehouse sink validated &          # validated.vehicle.*   -> validated.validated_<type>
.venv/bin/python -m lakehouse sink quarantine &         # quarantine.vehicle    -> quarantine.vehicle
.venv/bin/python -m lakehouse curate                    # validated -> curated, repeats dropped
.venv/bin/python -m lakehouse status                    # rows, files, snapshots per table
```

| Command | Does |
|---|---|
| `bootstrap [--reset]` | Create namespaces and tables. Never alters an existing table; reports `drifted` if its columns differ from the models |
| `sink raw\|validated\|quarantine` | The Kafka -> Iceberg writers. `--commit-interval`, `--batch-rows`, `--idle-timeout` |
| `curate [TYPE ...]` | Incremental validated -> curated |
| `compact [TABLE ...] [--benchmark]` | Merge small files; with `--benchmark`, time the standard queries before and after |
| `expire [TABLE ...] --older-than 1h` | Drop old snapshots (metadata only) |
| `orphans [TABLE ...] --older-than 1h [--dry-run]` | Delete files no snapshot references |
| `demo evolve\|timetravel\|retention\|all` | Iceberg features on scratch tables in the `demo` namespace |
| `status`, `facts` | Counts from metadata; contents summary used by the end-to-end test |
| `query [NAME]`, `sql "..."` | Run a saved query (`queries/NAME.sql`) or any SQL on Trino |
| `backfill DIR ... [--namespace bench]` | Load simulator JSONL runs into curated-shaped tables (multi-day benchmark data) |

Consoles: MinIO http://127.0.0.1:9001 (`minioadmin` / `minioadmin`), catalog http://127.0.0.1:8181, Trino
http://127.0.0.1:8082 (monitoring only), CloudBeaver SQL editor http://127.0.0.1:8978 (`cbadmin` / `Lakehouse1`).
Settings come from `LAKEHOUSE_*`, `MINIO_ROOT_*`, `KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL` (see `config.py`).
Prometheus metrics: sinks serve `lakehouse_rows_written_total`, `lakehouse_commits_total`,
`lakehouse_commit_seconds`, `lakehouse_decode_errors_total` and `lakehouse_duplicate_rows_total`
on ports 9109 (raw), 9110 (validated) and 9111 (quarantine).

## Layers

```
vehicle.<type> ───────────────► raw.raw_<type>                 as received: bytes + Kafka coordinates
     │ validator
     ├─ valid ► validated.vehicle.<type> ► validated.validated_<type> ──curate──► curated.<name>
     └─ invalid ► quarantine.vehicle ────► quarantine.vehicle         original bytes + reason
```

| Namespace | Tables | What it promises |
|---|---|---|
| `raw` | `raw_telemetry`, `raw_perception`, `raw_planning`, `raw_health`, `raw_software` | Every message that reached `vehicle.*`, untouched: `payload` is the original bytes, `vehicle_id` the message key, `timestamp` the Kafka record time (the simulator sets it to event time). Nothing is rejected, so corrupt records are here too |
| `validated` | `validated_<type>` (5) | Typed rows that passed the contract. **May hold repeats**: the validator's dedup state is in memory, so after a restart it can emit an event it already sent |
| `curated` | `vehicle_perception`, `vehicle_planning`, `vehicle_telemetry`, `vehicle_health`, `software_deployments` | The validated rows with each `event_id` once, plus `curated_time`. Analytics should read this layer |
| `quarantine` | `vehicle` | The records the validator rejected: `payload` (original bytes), `reason`, `detail`, `source_topic`, `source_partition`, `source_offset` |

The typed schemas are generated from the Pydantic models in `schemas/` (`tables.py`), so a field added to a
model appears in the tables without a second definition. Iceberg tracks columns by id, so ids follow model
order: new fields are appended, never reordered. Typed tables add `late`, `kafka_partition`, `kafka_offset`
and `ingest_time` (when the sink read the record).

`curate` is incremental. It stores a watermark (the newest `ingest_time` it has copied) in the curated
table's properties and writes it in the same commit as the rows, so a crash cannot lose or repeat rows.
Repeats are found by `event_id`; both copies carry the same event time, so the check against what is already
curated reads only the new batch's time range. The watermark is safe because one sink feeds each validated table
and stamps `ingest_time` before the commit: a later commit never contains a row older than an earlier one's newest.

## Partitioning

Every table except quarantine is partitioned by `day(timestamp)` and `bucket(8, vehicle_id)`; quarantine by day only.

- **Day** matches how queries filter and lets Iceberg skip whole days.
- **Bucket on `vehicle_id`** keeps a vehicle's rows together, as the Kafka partition key does, so per-vehicle lookups
  and the per-vehicle temporal joins in Phase 6 read one bucket instead of all of them. These hashes are Iceberg's
  (murmur3), not Kafka's, so buckets and Kafka partitions do not line up and need not.
- **Why 8.** A commit writes one file per (day, bucket) it touches, so N bounds the files per commit: 8 buckets
  is 8 files per table per day-commit. At 1,000 vehicles that is about 125 vehicles per bucket. More buckets would
  multiply the small-file problem below; fewer would make each lookup read more. N is fixed per partition spec;
  changing it is a partition evolution (see the `evolve` demo): old files keep the old layout.
- Quarantine has no usable `vehicle_id` or event time on corrupt records, so it partitions on the Kafka record time only.

## Delivery and restart

Each sink is its own consumer group and process. Each commit writes the last Kafka offset per partition into
the table properties (`kafka.offset.<topic>.<partition>`), **in the same atomic Iceberg commit as the data**.
On start the sink seeks every partition to the offset after the one its table records. The Kafka group offset
is committed afterwards only to show lag in Kafka tooling; it is never used to resume.

- A crash between the Iceberg commit and a Kafka commit replays nothing. The table, not Kafka, is the source of truth.
- A crash before the Iceberg commit replays the uncommitted batch from Kafka. Nothing was written, so nothing repeats.
- The same event reaching a table twice **within one commit** is dropped by `event_id`. Across commits it is not,
  which is what `curate` is for.
- A record that cannot be decoded is counted (`lakehouse_decode_errors_total`), logged and skipped, and its offset
  still advances, so one poison record cannot stop the stream. Since the validator wrote the validated topics, this
  should not happen; the raw and quarantine sinks store bytes and cannot fail to decode.
- The offsets are plain table properties, readable by any engine. A Spark writer can start where a PyIceberg
  sink stopped: pass those offsets as `startingOffsets`.

Concurrent writers to one table are safe: a commit that loses a race is retried against the new table state
(`CommitFailedException`, three attempts). Compaction and the sinks can run together for that reason.

## Small files, compaction and retention

Every commit writes at least one Parquet file per partition it touches. A sink that commits every few seconds
therefore turns a day of data into thousands of files of a few KB. Planning a query then opens every manifest
and every file footer, and the cost grows with file count, not data size. `--commit-interval` and `--batch-rows`
control the commit size; `lakehouse/smallfiles.sh` sets `--batch-rows 1000` on purpose to make the problem.

`compact` rewrites every partition that holds at least `--min-files` (4) files under `--small-file-bytes` (32 MB),
sorted by `(vehicle_id, timestamp)` so each file's min/max statistics are tight and prune well. It is a single
atomic commit per table, so readers see the old files or the new ones. It preserves Kafka offsets (they live in
table properties, not in files).

Compaction only adds a snapshot: the old small files stay referenced by earlier snapshots, which is what lets you
time travel. They are freed in two steps, and the order matters:

1. `expire` removes snapshots older than the cutoff (always keeping the newest). Metadata only; storage is unchanged.
2. `orphans` deletes files under the table that no remaining snapshot, manifest or metadata file references.
   `--older-than` protects files a writer has produced but not yet committed, so do not set it to `0s` while
   sinks run.

The REST server in `docker-compose.yml` (Iceberg 1.6) rejects a `remove-snapshots` update with several ids, so
`expire` commits one snapshot at a time.

Results from `lakehouse/smallfiles.sh` are in [`architecture/benchmarks.md`](../architecture/benchmarks.md):
about 850 files became 8 and queries ran 5 to 31 times faster.

## Iceberg features (`demo`)

| Demo | Shows |
|---|---|
| `evolve` | Add `occlusion` (perception v2): existing rows read it as null, no file rewritten. Rename a column: values unchanged, because columns are tracked by id. Change `bucket(8)` to `bucket(16)`: old files keep the old spec, new files use the new one, queries span both |
| `timetravel` | Read the table at a snapshot id and at a timestamp. After compaction, the pre-compaction snapshot still reads the same rows from different files |
| `retention` | Expire 10 snapshots: time travel to them ends, but storage is unchanged. Remove orphans: objects drop by about three quarters and the table reads the same rows |

## Tests

```
.venv/bin/python -m pytest tests/test_lakehouse.py     # no containers: local SQL catalog + filesystem warehouse
lakehouse/e2e.sh                                       # full stack: 13 checks, see below
lakehouse/smallfiles.sh                                # the compaction experiment, 4 checks
lakehouse/trino_bench.sh                               # Trino pruning, join and pre-aggregation benchmarks
lakehouse/milestone1.sh                                # Milestone 1 exit check: 10 simulated minutes -> Trino, 6 checks
```

`e2e.sh` runs the simulator with 5% bad data, then checks that: raw holds every produced record; validated holds
every valid one; quarantine holds every rejected one; valid + duplicates + quarantined = raw; each injected
corruption type is in quarantine with the right reason and count; every validated and curated row passes its
Pydantic model (the Milestone 2 criterion: zero corrupt rows in validated tables); curated holds each event once;
and a second pass of every sink and `curate` writes nothing new.

## Operating notes

- **The catalog is Postgres**, not SQLite. With SQLite, three sinks committing at once hit `database is locked`,
  which surfaces as `CommitStateUnknownException`: the client cannot tell whether the commit landed. Use a real database.
- **MinIO image.** MinIO no longer publishes images to Docker Hub or Quay. `docker-compose.yml` uses the last
  community build (`bitnamilegacy/minio`), which also creates the bucket at start. It is frozen: fine locally.
- **PyIceberg limits (0.10):** no built-in compaction or orphan removal (implemented in `maintenance.py`), no
  manifest rewrite, and `table.maintenance.expire_snapshots()` declares its id set on the class, so `maintenance.py`
  resets it per use.
- **Moving to Spark later.** The tables, catalog and offsets-in-properties design are engine-neutral. Replace the
  sinks with Structured Streaming and start from the stored offsets; `rows.py` holds the row mapping as pure functions.
  Maintenance becomes `CALL system.rewrite_data_files`, `expire_snapshots` and `remove_orphan_files`.
