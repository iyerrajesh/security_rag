# Architecture

Target architecture for the Autonomous Systems Data & AI Platform. ✅ marks what
exists today; everything else is planned (see [`docs/plan.md`](../docs/plan.md)).

```mermaid
flowchart TD
    SIM["✅ Fleet simulator<br/>simulator/"] -->|JSONL files ✅| FILES[(Local JSONL)]
    SIM -->|"producer, key = vehicle_id"| KAFKA["Kafka (KRaft)<br/>vehicle.telemetry · perception · planning · health · software"]
    SCHEMAS["✅ Pydantic schemas<br/>schemas/"] -.validates.-> SIM
    SCHEMAS -.validates.-> VAL

    KAFKA --> VAL["Schema validation +<br/>data quality"]
    VAL -->|invalid| QUAR[(Quarantine topic)]
    VAL -->|valid| VTOP[(validated.* topics)]
    VAL --> FLINK["Flink<br/>streaming correlation"]

    KAFKA -->|"✅ sink raw"| ICE
    QUAR -->|"✅ sink quarantine"| ICE
    VTOP -->|"✅ sink validated"| ICE[("✅ Iceberg lakehouse on MinIO<br/>raw → validated → curated · quarantine")]
    FLINK --> ICE
    ICE --> TRINO["Trino<br/>analytics"]
    ICE --> MINING["Scenario mining"]
    MINING --> DATASETS["ML training datasets"] --> MLFLOW["MLflow"]
    MINING --> EMBED["Scenario text + embeddings"]
    EMBED --> PGV[("✅ Postgres + pgvector")]
    PGV --> RAG["✅ RAG / AI assistant<br/>(app/, tool-based)"]
    TRINO --> RAG

    OBS["Prometheus · Grafana · OpenLineage/Marquez"] -.observes.-> KAFKA & VAL & ICE & RAG
```

Plain-text version:

```text
Fleet simulator ──► Kafka ──► Validation / DQ ──┬──► Quarantine ──► Iceberg quarantine (✅)
     (✅)             │                          ├──► validated.* ──► Iceberg validated → curated (✅) ──► Trino ──┐
                      └──► Iceberg raw (✅)      └──► Flink ───────────────┘                                      │
                                                        Iceberg ──► Scenario mining ──┬─► ML datasets ──► MLflow
                                                                                      └─► Embeddings ──► pgvector (✅) ──► RAG (✅)
```

## Components

| Layer | Component | Status | Notes |
|---|---|---|---|
| Data model | `schemas/` Pydantic models | ✅ | Single source of truth; see [data-model.md](data-model.md) |
| Generation | `simulator/` | ✅ | State-machine vehicles, 9 scenarios, planted failure chain, JSONL output |
| Ingestion | Kafka (single-node KRaft) | ✅ | See [ingestion/README.md](../ingestion/README.md) |
| Data contracts | Schema Registry, validator, quarantine | ✅ | See [validation/README.md](../validation/README.md) |
| Storage | MinIO + Iceberg (REST catalog on Postgres), PyIceberg writers | ✅ | `raw` / `validated` / `curated` / `quarantine` namespaces; compaction and retention; see [lakehouse/README.md](../lakehouse/README.md) |
| Analytics | Trino | Phase 6 | |
| Streaming | PyFlink | Phase 9 | Batch scenario mining first |
| ML | Scenario mining, datasets, MLflow | Phases 7–8, 10 | |
| AI | pgvector + RAG with tool use | ✅ base exists in `app/` | Moves to `rag/` in Milestone 4 |
| Ops | Prometheus, Grafana, OpenLineage | Phase 12 | |

## Key decisions so far

- **Partition key: `vehicle_id`** everywhere (Kafka partitions, Iceberg buckets).
  This preserves per-vehicle ordering and makes per-vehicle temporal joins local.
- **Event time, not ingest time.** Every event carries a UTC `timestamp`.
  Late and out-of-order events are expected and handled downstream.
- **Idempotency through `event_id`.** Replaying Kafka must not duplicate rows. The lakehouse sinks resume from
  offsets stored in the table, in the same commit as the data; `curate` removes the repeats the validator can emit.
- **The table is the source of truth for ingest progress**, not the Kafka consumer group. A sink restart replays nothing.
- **One local environment.** Platform services run as Compose profiles (`m1`, `m3`, …),
  so the production RAG deploy (`docker compose up` with no profile) is unaffected.
- **Iceberg REST catalog backed by Postgres.** SQLite locked under three concurrent writers.
- **Layers mean something.** `raw` is untrusted bytes, `validated` is typed but may hold repeats, `curated` is
  deduplicated and is what analytics reads, `quarantine` is what the validator rejected.
- **pgvector instead of OpenSearch** for vectors. It is already in the repo and
  uses about 2 GB less RAM.
