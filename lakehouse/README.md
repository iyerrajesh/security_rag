# Lakehouse (Phase 5)

Kafka -> Iceberg tables on MinIO, through an Iceberg REST catalog. Work in progress:
the writers and tables are built; compaction, benchmarks and the Iceberg feature
demos come next.

```
docker compose --profile m1 up -d --wait minio iceberg-rest schema-registry kafka kafka-init
python -m lakehouse bootstrap                  # namespaces + tables (idempotent; --reset drops them)
python -m lakehouse sink raw &                 # vehicle.*             -> raw.raw_<type>
python -m lakehouse sink validated &           # validated.vehicle.*   -> validated.* / curated.*
python -m lakehouse sink quarantine &          # quarantine.vehicle    -> quarantine.vehicle
python -m lakehouse status                     # rows, files, snapshots per table
```

MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`). Catalog: http://127.0.0.1:8181.
Settings come from `LAKEHOUSE_*`, `MINIO_ROOT_*`, `KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL` (see `config.py`).

| Table | Fed from | Holds |
|---|---|---|
| `raw.raw_<type>` (5) | `vehicle.<type>` | Message bytes as received, key, Kafka coordinates. `timestamp` is the Kafka record time |
| `validated.validated_perception`, `validated.validated_planning` | `validated.vehicle.<type>` | Typed events |
| `curated.vehicle_telemetry`, `vehicle_health`, `software_deployments` | `validated.vehicle.<type>` | Typed events |
| `quarantine.vehicle` | `quarantine.vehicle` | Original bytes, `reason`, `detail`, source topic/partition/offset |

Every table is partitioned by `day(timestamp)` and `bucket(8, vehicle_id)` (quarantine: day only).
Typed schemas are generated from the Pydantic models in `schemas/`.

**Progress and replay.** Each commit stores the last Kafka offset per partition in the table's
properties (`kafka.offset.<topic>.<partition>`), atomically with the data. A restarted sink
resumes from those, not from the consumer group, so a crash replays nothing. Another writer
(e.g. Spark) can start from the same properties.

**Small files.** `--commit-interval` (default 10 s) and `--batch-rows` control commit size. Each
commit writes at least one file per partition touched.
