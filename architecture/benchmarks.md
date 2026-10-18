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

