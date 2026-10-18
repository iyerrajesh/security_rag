# Ingestion (Kafka)

A single-node Kafka 3.8 cluster in KRaft mode (no ZooKeeper), run locally with
Docker Compose. The simulator produces to it; consumers land and process the
events downstream.

```bash
docker compose --profile m1 up -d kafka kafka-init        # broker + topic creation
simulator/run.sh --sink kafka --vehicles 100 --duration 60
.venv/bin/python -m ingestion.consume --from-beginning    # verify what arrived
```

The services use the `m1` profile, so the production `docker compose up` (the
RAG API deploy) never starts them. Use `127.0.0.1:9094` from the host and
`kafka:9092` from containers on the compose network.

## Topics

| Topic | Event | Partitions | Retention |
|---|---|---|---|
| `vehicle.telemetry` | `TelemetryEvent` | 6 | 7 days |
| `vehicle.perception` | `PerceptionEvent` | 6 | 7 days |
| `vehicle.planning` | `PlanningEvent` | 6 | 7 days |
| `vehicle.health` | `HealthEvent` | 6 | 7 days |
| `vehicle.software` | `SoftwareEvent` | 6 | 7 days |
| `validated.vehicle.<type>` | Avro, after validation | 6 | 7 days |
| `quarantine.vehicle` | Rejected raw records with reason headers | 6 | 30 days |

The `validated.*` and `quarantine.vehicle` topics are written by the Phase 4
validator ([`validation/`](../validation/README.md)).

[`infrastructure/kafka/create-topics.sh`](../infrastructure/kafka/create-topics.sh)
creates the topics and is safe to re-run. Auto-creation is turned off, so a typo
in a topic name fails loudly instead of quietly creating a new topic.

## Message format

| Part | Value |
|---|---|
| Key | `vehicle_id` (UTF-8) |
| Value | Event JSON, exactly as serialized by the `schemas/` model |
| Timestamp | The event's `timestamp` (event time, not send time) |

Raw `vehicle.*` topics stay JSON on purpose. They stand in for untrusted edge
input, which is where bad data comes from. The validator turns raw records into
Avro on `validated.vehicle.*`, using the schema registry, or quarantines them.

## Design notes

### Partitioning

Messages are keyed by `vehicle_id`, so the default hash partitioner sends all of
one vehicle's events in a topic to one partition. Because every topic uses the
same key and partition count, a vehicle lands on the **same partition number in
every topic**. A consumer joining telemetry, perception and planning for a
vehicle can then read matching partitions with no shuffle. The producer uses `murmur2_random`, the Java client's partitioner, so Python and JVM producers (Flink in Phase 9) map keys to the same partitions. Six partitions allow
up to six parallel consumers per group. That is plenty on a laptop, and
partitions can be added later. Adding partitions remaps keys, though, so the
count is fixed before real data matters.

### Ordering

Kafka orders messages only within a partition. Keying by vehicle therefore
gives exactly the guarantee we need: each vehicle's events are in order. There
is no ordering across vehicles. The producer has `enable.idempotence=true`
(which implies `acks=all`), so retries can't reorder or duplicate messages
within a partition. Ordering across topics is **not** guaranteed: a vehicle's
planning event may arrive before its matching telemetry. Correlation must
therefore use event time with windows, never arrival order.

### Consumer groups

Each downstream job uses its own group. Examples are the lakehouse sinks (`lakehouse-raw`,
`lakehouse-validated`, `lakehouse-quarantine`), the Phase 4 validator, and the Phase 9 Flink jobs. Each group reads the full
stream independently and tracks its own offsets. Within a group, partitions are
divided among its consumers. The `ingestion.consume` verifier uses a new random
group each time, so it never moves offsets for real consumers.

### Replay

Retention is time-based, not deletion on consumption, so any group can rewind:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group <group> --reset-offsets --to-earliest --all-topics --execute
```

(`--to-datetime 2026-10-04T10:00:00.000` also works.) A replay delivers events a
second time, so every sink must be idempotent. Downstream writes deduplicate on
`event_id`.

### Retention

> **Retention counts from event time.** Message timestamps are event time, and time-based retention measures age from that timestamp. Events dated more than 7 days ago are deleted almost as soon as they land. For that reason, `--sink kafka` starts the simulated clock at *now* by default. If you pass an old `--start-time`, `ingestion.consume` reports 0 messages.

Topics keep 7 days (`retention.ms=604800000`) with `cleanup.policy=delete`.
Kafka is the replay buffer; the lakehouse is the system of record (Phase 5).
The 7-day window is long enough to rebuild after a broken downstream deploy.
Compaction would make no sense here: these are events, not latest-value state.

### Throughput

The producer batches (`linger.ms=20`, `batch.size=256 KB`) and compresses
(`lz4`). Topics use `compression.type=producer`, so the broker stores batches
as sent instead of recompressing them.

Measurements on a laptop (Apple Silicon, 1 GB broker heap, 1 Hz report rate):

| Run | Events | Produce | Consume + validate |
|---|---|---|---|
| 100 vehicles × 60 s | 44,979 | ≈ simulator rate | — |
| 1,000 vehicles × 60 s | 451,468 | 25k events/s (18 s) | 160k msg/s |

Producing runs at about the simulator's own rate: the simulator generates about
29k events/s when writing JSONL. Python event generation is the bottleneck, not
Kafka. A live 10 Hz feed from 1,000 vehicles (about 75k events/s) will need
several simulator processes in parallel. The Phase 3 benchmarks go in
`architecture/benchmarks.md`.

### Backpressure

If the broker is slower than the producer, librdkafka's local queue fills up and
`produce()` raises `BufferError`. `KafkaSink` catches it and calls `poll()` to
wait for deliveries to free space, which blocks the simulator. Nothing is
dropped, and memory stays bounded. On shutdown, `flush()` waits for everything
outstanding. The run fails loudly if any message was never delivered.
Consumers pull at their own pace, so a slow consumer builds up lag rather than
backpressure. Lag is the metric to alert on in Phase 12.

These behaviours are covered by repeatable tests in
[`infrastructure/chaos/`](../infrastructure/chaos/README.md): broker pause, message
timeout, bounded queue, broker throttling, slow consumer, and consumer crash/restart.

## Status

- [x] Single-node KRaft broker, topic creation, Kafka UI (`docker compose --profile m1 up -d`)
- [x] Producer sink (`simulator --sink kafka`) and verifier consumer (`ingestion.consume`)
- [x] Throughput benchmark at 100 and 1,000 vehicles in [`architecture/benchmarks.md`](../architecture/benchmarks.md)
- [x] Backpressure and failure tests in [`infrastructure/chaos/`](../infrastructure/chaos/README.md)
- [x] Consumer that lands raw events in MinIO. Done in Phase 5: `python -m lakehouse sink raw` writes
      `raw.raw_<type>` Iceberg tables (see [`lakehouse/README.md`](../lakehouse/README.md)).
