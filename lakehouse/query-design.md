# Lakehouse query design

How to query the lakehouse with Trino, and why the saved queries are written the way they are. Numbers are in
[`architecture/benchmarks.md`](../architecture/benchmarks.md) (Phase 6).

```
docker compose --profile m1 up -d --wait kafka iceberg-rest minio trino   # Trino UI/SQL: http://127.0.0.1:8082
.venv/bin/python -m lakehouse query                              # list the saved queries
.venv/bin/python -m lakehouse query failure_rate_by_version
.venv/bin/python -m lakehouse sql "SELECT count(*) FROM curated.vehicle_perception"
```

For a browser SQL editor, start CloudBeaver: `docker compose --profile m1 up -d cloudbeaver`, open
http://127.0.0.1:8978 and sign in as `cbadmin` / `Lakehouse1` (override with `CB_ADMIN_NAME` and `CB_ADMIN_PASSWORD`).
The connection **Lakehouse (Trino)** is already set up; open it and use the SQL editor (Ctrl+Enter runs a statement).
Trino's own UI on the same port as the API only monitors queries: it has no editor, and the preview UI in Trino 476
does not add one. CloudBeaver uses about 250 MB.

Trino's catalog is `lakehouse`; its schemas are the Iceberg namespaces, so tables read `curated.vehicle_perception`.
Trino reads the tables the PyIceberg sinks write, through the same REST catalog. The only thing it writes is the
pre-aggregation table. Configuration is in [`infrastructure/trino/etc/`](../infrastructure/trino/etc).

## Which layer to query

| Layer | Query it for | Because |
|---|---|---|
| `curated.*` | Analytics | Each `event_id` once. The validator can emit an event twice after a restart, so `validated` can double-count |
| `validated.*` | Debugging the validator | Has the repeats, plus `kafka_partition` / `kafka_offset` to trace a row back to Kafka |
| `raw.*` | Reprocessing, forensics | The bytes as received; use `from_utf8(payload)` and `json_extract` |
| `quarantine.vehicle` | Data quality | `reason`, `detail` and the source topic, partition and offset |

## The saved queries

Each is in [`queries/`](queries) with a comment explaining what it measures.

| Query | Question | Technique |
|---|---|---|
| `failure_rate_by_version` | Which software, perception and firmware versions fail most? | As-of join to `software_deployments` |
| `pedestrian_confidence_by_firmware` | Do pedestrians get detected less confidently on some firmware? | As-of join; share of detections below the planner's 0.5 trust threshold |
| `pedestrian_detection_failures` | Which weak pedestrian detections preceded an emergency brake, and what was the vehicle doing? | Three-way temporal join on `vehicle_id` + time window |
| `correlated_events` | The planner's decision and the vehicle's motion at each pedestrian detection (the Milestone 1 exit query) | Same, with the nearest rows picked by `min_by` |
| `quarantine_reasons` | Why was data rejected? | Group by reason and source topic |
| `build_perception_hourly` | Pre-aggregate detections per vehicle, hour and type | `CREATE TABLE AS` |

### Joining across event types: vehicle_id + time window

The five event streams share only `vehicle_id` and an event-time `timestamp`. They are not ordered relative to each
other (planning can arrive before its telemetry) and a vehicle reports each stream at its own rate. So correlation
is never on arrival order or an exact timestamp. It is an equality on `vehicle_id` and a range on time:

```sql
JOIN curated.vehicle_planning pl
  ON pl.vehicle_id = p.vehicle_id
 AND pl."timestamp" BETWEEN p."timestamp" - INTERVAL '1' SECOND AND p."timestamp" + INTERVAL '1' SECOND
```

The equality lets Trino hash-partition both sides by vehicle, so the time range is a filter inside each vehicle's
rows, not a cross product. A window usually matches several rows (reports are about 1 per second), so the queries
collapse them with `min_by(value, abs(date_diff('millisecond', other.timestamp, p.timestamp)))`: the value from
the row nearest in time.

### As-of join for versions

A vehicle runs one version at a time, and `software_deployments` records when each began. To attach the version in
force to an event, turn the deployment events into intervals with `lead`, then join on the interval:

```sql
SELECT vehicle_id, "timestamp" AS valid_from,
       lead("timestamp") OVER (PARTITION BY vehicle_id ORDER BY "timestamp") AS valid_to, ...
...
JOIN versions v ON e.vehicle_id = v.vehicle_id AND e."timestamp" >= v.valid_from
               AND (v.valid_to IS NULL OR e."timestamp" < v.valid_to)
```

An event before a vehicle's first recorded deployment has no interval and drops out of an inner join.

### Defining "failure"

There is no incidents table: the platform records events, not verdicts. Failure is therefore defined in the query,
in measurable terms, and stated in its header comment:

- **Emergency braking**: `planning.emergency_braking`.
- **Camera fault**: `health.camera_status <> 'ok'`.
- **Weak pedestrian detection**: a pedestrian with confidence below 0.5, which the planner does not act on until
  lidar confirms it at about 20 m.
- **Pedestrian detection failure**: a weak detection within 40 m followed by an emergency brake on the same
  vehicle within 3 seconds.

Change a definition by editing the one query; nothing downstream depends on a stored label.

## Writing queries that prune

What the tables are laid out for (details in [`README.md`](README.md#partitioning)): partitioned by `day(timestamp)`
and `bucket(8, vehicle_id)`, files sorted by `(vehicle_id, timestamp)` after compaction.

1. **Filter on `timestamp` with a range**, written against the bare column: `"timestamp" >= TIMESTAMP '...' AND
   "timestamp" < ...`. Trino turns that into a day filter and skips the other days' files. A function on the column
   (`date(timestamp) = ...`) can hide it from pruning. `timestamp` is quoted throughout because it is also a type name.
2. **Filter on `vehicle_id` with `=` or `IN`** to read one bucket of eight per day. A `LIKE` does not prune.
3. **Select only the columns you need.** Parquet is columnar, so unread columns are not decoded, and (see the next
   point) not even read from storage on well-sized files.
4. **Keep files above 3 MB.** Trino reads any Parquet file smaller than `parquet.small-file-threshold` (3 MB) whole,
   so column pruning saves no I/O on small files: in the benchmark a one-column query read exactly as many bytes as a
   ten-column one. Compacted files (128 MB target) do not have this problem. Lowering the threshold is possible
   (session property `parquet_small_file_threshold`) but trades one large read for many small ones.
5. **Put the time filter on every table in a join.** A filter on one table does not prune the others. In the
   benchmark only perception and planning carried the day filter, and the telemetry scan still read all 56 files.
6. **Pre-aggregate repeated summaries.** An hourly confidence chart should not rescan every detection. Store sums and
   counts, not averages, so the summary can be re-aggregated to a coarser grain. The price is a table to refresh.
7. **Return fewer rows.** `ORDER BY ... LIMIT n` still sorts everything below it; filter first.

## Checking what a query did

```sql
EXPLAIN ANALYZE SELECT ...
```

In the table-scan lines, `Splits` is the number of files read, `Physical input` the bytes fetched from storage and
`Input` the rows. Pruning shows up there as smaller numbers, long before it shows up in the elapsed time.
`python -m lakehouse.trino_bench` collects these for each experiment.

## Operating notes

- **Memory.** Trino has a 2 GB heap and a 1 GB per-query limit (`infrastructure/trino/etc`). On an 8 GB Docker VM it
  shares memory with Kafka, the schema registry and MinIO; stop `kafka-ui` if it gets tight.
- **Ports.** Trino is on 8082, because 8080 is Kafka UI; CloudBeaver is on 8978.
- **CloudBeaver settings** live in its `cloudbeaverdata` volume and are seeded once from
  [`infrastructure/cloudbeaver/data-sources.json`](../infrastructure/cloudbeaver/data-sources.json). To re-seed after
  editing that file, remove the service and the volume (`docker volume rm security_rag_cloudbeaverdata`).
- **Credentials** are read from the `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` environment variables, like the sinks.
- **Pre-aggregation table.** `curated.perception_hourly` is created by Trino, not by `bootstrap`, so `bootstrap
  --reset` does not drop it. Run `DROP TABLE IF EXISTS curated.perception_hourly` before rebuilding it.
- **Benchmark data** is in the `bench` namespace, loaded from simulator files by `python -m lakehouse backfill`.
  Drop its tables in Trino to remove it.
