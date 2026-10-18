# Benchmarks

## Phase 3 — Kafka ingestion

**Setup:** single-node Kafka 3.8 (KRaft, 1 GB heap) in Docker on an Apple Silicon laptop.
Simulator runs used the defaults: seed 42, 60 s simulated, 1 Hz report rate per
vehicle. The consumer is `ingestion.consume` with group `test-c1`; it validates
every message against `schemas/`.

```bash
simulator/run.sh --sink kafka --vehicles <N> --duration 60
.venv/bin/python -m ingestion.consume --group-id test-c1
```

### Throughput

| Vehicles | Events | Produce time | Produce rate | Consume time | Consume rate | Invalid | Key mismatch | Out of order |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 44,979 | 1.7 s | 26,305 events/s | 0.3 s | 146,940 msg/s | 0 | 0 | 0 |
| 1,000 | 451,468 | 17.8 s | 25,344 events/s | 3.6 s | 123,701 msg/s | 0 | 0 | 0 |

The produce rate stays flat as the fleet grows tenfold: Python event generation
is the bottleneck, not Kafka. Every produced event was consumed, in both runs.

### Events per topic

| Topic | 100 vehicles | 1,000 vehicles |
|---|---:|---:|
| `vehicle.telemetry` | 6,000 | 60,000 |
| `vehicle.perception` | 26,854 | 270,181 |
| `vehicle.planning` | 6,000 | 60,000 |
| `vehicle.health` | 6,000 | 60,000 |
| `vehicle.software` | 125 | 1,287 |
| **Total** | **44,979** | **451,468** |

### Scenario episodes (ground truth, not sent to Kafka)

| Scenario | 100 vehicles | 1,000 vehicles |
|---|---:|---:|
| `pedestrian_crossing` | 123 | 1,240 |
| `camera_degradation` | 98 | 975 |
| `hard_braking` | 41 | 407 |
| `software_deployment` | 25 | 287 |
| `night` | 28 | 230 |
| `rain` | 22 | 209 |
| `network_degradation` | 16 | 187 |
| `construction_zone` | 18 | 162 |
| `sensor_failure` | 4 | 105 |

## Phase 5 — Compaction

**Setup:** `lakehouse/smallfiles.sh`: 200 vehicles, 120 s simulated, 5% bad data (185,184 events). Iceberg tables on
MinIO through the REST catalog (Postgres-backed), Apple M2 Pro. The sinks commit every 1,000 rows, on purpose, so each
commit adds one tiny Parquet file per (day, vehicle bucket) partition. Then `python -m lakehouse compact --benchmark`
times three queries through PyIceberg (plan files, then read with PyArrow) before and after. Each query runs once to
warm up, then five times; the table shows the median.

```bash
lakehouse/smallfiles.sh        # VEHICLES, DURATION, BATCH_ROWS and TABLES can be overridden
```

### Files

| Table | Rows | Files before | Files after | Size before | Size after |
|---|---:|---:|---:|---:|---:|
| `raw.raw_perception` | 112,341 | 879 | 8 | 8.4 MB | 5.1 MB |
| `validated.validated_perception` | 107,414 | 833 | 8 | 10.2 MB | 4.0 MB |

Eight files is one per (day, bucket) partition: the simulated run fits in one day. The size drop is partly
per-file overhead (each tiny file carries its own footer and statistics) and partly sorting by
`(vehicle_id, timestamp)` before the rewrite, which compresses better.

### Query time (median of 5, ms)

| Table | Query | Before | After | Speed-up |
|---|---|---:|---:|---:|
| `raw_perception` | plan files | 178.1 | 27.3 | 6.5x |
| | `count_rows` (one column, full scan) | 737.3 | 31.5 | 23.4x |
| | `one_vehicle` (one vehicle, one hour) | 345.4 | 64.9 | 5.3x |
| `validated_perception` | plan files | 178.0 | 30.2 | 5.9x |
| | `count_rows` | 954.8 | 38.0 | 25.1x |
| | `one_vehicle` | 388.4 | 70.4 | 5.5x |
| | `avg_confidence_by_type` (group by, 2 columns) | 1,159.8 | 37.4 | 31.0x |

The full scans gain the most because they open every file. The lookup gains less, probably because bucket and
file-statistics pruning already skipped most of the small files (not measured separately). The gain grows with file count, not
data size: this dataset is only 10 MB, so the same few-KB-per-commit pattern over a real day would be far worse.
These are PyIceberg timings, not Trino's; Phase 6 repeats the comparison with `EXPLAIN ANALYZE`.

### Retention

After compaction, `expire` removed 114 (`raw_perception`) and 108 (`validated_perception`) snapshots, and `orphans`
deleted about 1,200 files from each table. Row counts were unchanged at every step (checked by the script). The
`retention` demo shows the same on a small table: 123 objects in storage, 133 after expiry (expiry only edits
metadata), 33 after orphan removal.

## Phase 6 — Trino

**Setup:** Trino 476, single node (2 GB heap), Iceberg connector over the REST catalog, tables on MinIO, all in Docker
on an Apple M2 Pro. `lakehouse/trino_bench.sh` simulates 7 days (100 vehicles, 10 simulated minutes per day, a
different seed per day) straight to files, loads them with `python -m lakehouse backfill` into the `bench`
namespace (same schema and layout as `curated`, so the live tables are untouched), then runs
`python -m lakehouse.trino_bench`. `bench.vehicle_perception` holds 1,999,348 rows in 56 Parquet files
(8 vehicle buckets x 7 days, 0.7 to 1.8 MB each). Kafka cannot supply multi-day data: retention counts from event
time, so records dated days ago are deleted as they arrive.

Each query runs once to warm up and then five times; the table shows the median. **Read the "rows", "bytes" and
"files" columns, not the milliseconds.** At 66 MB every query finishes in tens of milliseconds, dominated by Trino's
fixed planning and scheduling cost, so the timings barely separate and vary between runs (a second full run measured the all-days scan at 211 ms
and `correlated_events` at 639 ms, against 60 ms and 1,500 ms below). What pruning removes is I/O, and the
statistics show that exactly: they come from Trino's own query statistics and `EXPLAIN ANALYZE`.

```bash
lakehouse/trino_bench.sh        # DAYS, VEHICLES and DURATION can be overridden
```

### Partition pruning

`SELECT count(*), avg(confidence) FROM bench.vehicle_perception` with different filters. Tables are partitioned
by `day(timestamp)` and `bucket(8, vehicle_id)`.

| Filter | Files read | Rows read | Bytes read | Median |
|---|---:|---:|---:|---:|
| none (7 days, 100 vehicles) | 56 | 1,999,348 | 66.0 MB | 60 ms |
| one day | 8 | 286,461 | 9.4 MB | 45 ms |
| one vehicle | 7 | 340,519 | 11.2 MB | 64 ms |
| one vehicle, one day | 1 | 48,037 | 1.6 MB | 43 ms |

A day filter reads 1 day in 7 (8 of 56 files); an equality filter on `vehicle_id` reads one bucket of 8 per day
(7 files), a bit over 1/8 of the rows because 100 vehicles do not hash evenly into 8 buckets. Both together read a
single file. Trino prunes on the partition transform of a filter, with no scan of the other files.

### Parquet column pruning

Same table and rows; one column (`avg(confidence)`) against ten.

| Session | Query | Bytes read | Median |
|---|---|---:|---:|
| default | ten columns | 66.0 MB | 160 ms |
| default | one column | **66.0 MB** | 43 ms |
| `parquet_small_file_threshold = 0B` | ten columns | 68.3 MB | 141 ms |
| `parquet_small_file_threshold = 0B` | one column | **5.1 MB** | 44 ms |

This is not what the Parquet format promises. By default Trino reads **any Parquet file under 3 MB whole**
(`parquet.small-file-threshold`), because one large read beats several small ones on high-latency storage. Every
file in this table is under 3 MB, so a one-column query reads exactly as many bytes as a ten-column one. The time
still falls (less to decode), but the I/O does not. With the threshold at 0, it reads only the column chunks it needs:
5.1 MB instead of 68.3 MB, 13x less.

So column pruning only saves I/O once files are bigger than 3 MB. Combined with the Phase 5 result, this is a second
reason to compact: the small-file problem costs more than planning time. Properly sized files (the 128 MB target)
get column pruning with the default settings.

### Temporal join

`pedestrian_detection_failures`: low-confidence pedestrians (perception) joined to emergency braking within
3 seconds (planning), joined to the nearest telemetry, on `vehicle_id` and a time window.

| Filter | Scan splits (perception, planning, telemetry) | Rows read | Bytes read | Median |
|---|---|---:|---:|---:|
| none (7 days) | 56, 56, 56 | 3,260,710 | 95.6 MB | 349 ms |
| one day on perception and planning | 8, 8, 56 | 1,186,617 | 29.7 MB | 228 ms |

The perception and planning scans prune to one day. Telemetry is joined only through the braking time, which Trino
cannot turn into a partition filter here, so it still reads all 56 files. A day filter on the telemetry join too
would remove them; the saved query leaves it out so the three-way correlation stays readable.

### Pre-aggregation

Average confidence and weak-detection share per hour and object type, from the detail table against
`bench.perception_hourly` (one row per vehicle, hour and object type, built by
`lakehouse/queries/build_perception_hourly.sql` in 1.1 s).

| Source | Rows read | Bytes read | Median |
|---|---:|---:|---:|
| `vehicle_perception` (detail) | 1,999,348 | 66.0 MB | 222 ms |
| `perception_hourly` | 5,116 | 62 KB | 49 ms |

390 times fewer rows and about 1,000 times fewer bytes. The cost is a table to refresh, and the summary stores sums
and counts rather than averages so that it can be re-aggregated to any coarser grain (per hour, per day, all vehicles).

### Saved-query latency (7 days, 3.3 million rows)

| Query | Rows read | Median |
|---|---:|---:|
| `failure_rate_by_version` | 844,796 | 333 ms |
| `pedestrian_confidence_by_firmware` | 2,001,746 | 311 ms |
| `pedestrian_detection_failures` | 3,260,710 | 411 ms |
| `correlated_events` | 3,679,348 | 1,500 ms |

`correlated_events` is the slowest because it has no time filter and sorts the whole join to return the first 20 rows.

### Milestone 1 exit check

`lakehouse/milestone1.sh`: the simulator runs 100 vehicles for 10 minutes (464,683 events, 5% bad data) into
Kafka, the validator and the three sinks land them in Iceberg, and Trino returns the correlated rows. All six checks
pass, including the planted failure chain: firmware 3.2 against 3.1 has 3.8x the camera faults (39.2 vs 10.4 per 100
health events), 3.7x the weak pedestrian detections (25.6% vs 7.0%) and 5.7x the emergency braking
(0.34 vs 0.06 per 100 planning events).

