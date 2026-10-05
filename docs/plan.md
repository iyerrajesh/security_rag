# Execution Plan — Autonomous Systems Data & AI Platform

Companion to [autonomous_systems_data_ai_platform.md](autonomous_systems_data_ai_platform.md).
The source document describes 14 phases grouped into 5 milestones over 10 weeks.
This plan maps every phase to concrete tasks, exit criteria, and build order.

---

## 0. How the existing repo fits

This repo is already a working pgvector + FastAPI + Voyage + Anthropic RAG service.
That is roughly 40% of Milestone 4 (Phases 10 and 11) built ahead of schedule.

Reusable as-is:

- `app/providers.py` — swappable embedding / completion provider abstraction
- `app/db.py` — psycopg3 connection pool
- `app/rag.py` — hybrid retrieve pattern (SQL filter + HNSW)
- `app/query_parser.py` — LLM query → structured filters

Decisions that follow:

- **Use pgvector, not OpenSearch**, for the vector index. The source doc leaves it open,
  the code already uses pgvector, and it saves ~2 GB RAM on a 16 GB laptop.
- **Adopt the Phase 14 repo layout now** (`simulator/`, `schemas/`, `ingestion/`,
  `validation/`, `streaming/`, `lakehouse/`, `scenario-mining/`, `ml/`, `embeddings/`,
  `rag/`, `api/`, `infrastructure/`, `dashboards/`, `architecture/`). Move the current
  `app/` into `rag/` when Milestone 4 begins. Restructuring early avoids a painful move in Week 10.

---

## 1. Milestone 1 — Data Platform (Weeks 1–4)

**Phases:** 1, 2, 3, 5, 6
**Goal:** simulator → Kafka → MinIO/Iceberg → Trino, queryable end to end.

### Phase 1 — Data model (Week 1)

- [ ] Write `architecture/data-model.md` covering the five event types
      (telemetry, perception, planning, health, software).
- [ ] Define each event type as a Pydantic model in `schemas/` so the simulator,
      validator, and Iceberg table schemas share one source of truth.
- [ ] Commit one example JSON record per event type.
- [ ] Commit the initial architecture diagram to `architecture/architecture.md`.

**Deliverables:** data model doc, event schemas, example JSON, architecture diagram.

### Phase 2 — Fleet simulator (Weeks 1–2)

- [ ] Build `simulator/` with a CLI: `--vehicles`, `--events-per-second`, `--duration`, `--seed`.
- [ ] Model each vehicle as a state machine so telemetry, perception, and planning
      stay physically consistent (speed, braking, trajectory changes).
- [ ] Add a scenario injector for the nine named scenarios: normal driving, pedestrian
      crossing, hard braking, sensor failure, network degradation, rain, night,
      construction zone, software deployment.
- [ ] Implement the correlated failure chain:
      firmware 3.2 → camera degradation → lower pedestrian confidence → emergency braking.
- [ ] First sink is JSONL files so the simulator is testable before Kafka exists.
- [ ] Generate and commit small sample datasets (100 vehicles, 60 s).

**Deliverables:** simulator, configurable rates, scenario generator, reproducible seed, sample data.

### Phase 3 — Kafka ingestion (Weeks 2–3)

- [ ] Add single-node Kafka (KRaft mode, no ZooKeeper) to `docker-compose.yml`.
- [ ] Create topics `vehicle.telemetry`, `vehicle.perception`, `vehicle.planning`,
      `vehicle.health`, `vehicle.software`, keyed/partitioned by `vehicle_id`.
- [ ] Write a Kafka producer sink for the simulator.
- [ ] Write a consumer that lands raw events to MinIO (prepares for Phase 5).
- [ ] Benchmark throughput at 100 and 1,000 vehicles; record in `architecture/benchmarks.md`.
- [ ] Document partitioning, ordering, consumer groups, replay, retention,
      throughput, and backpressure in `ingestion/README.md`.

**Deliverables:** Kafka deployment, producers, consumers, topic config, throughput benchmark, Kafka docs.

### Phase 5 — Lakehouse (Weeks 3–4)

- [ ] Add MinIO and an Iceberg REST catalog to compose; write with PyIceberg.
- [ ] Create `raw`, `validated`, `curated` namespaces.
- [ ] Create tables: `raw_perception`, `validated_perception`, `vehicle_telemetry`,
      `vehicle_health`, `software_deployments`.
- [ ] Partition strategy: `day(timestamp)` + `bucket(N, vehicle_id)`.
- [ ] Deliberately create a small-file problem (commit every few seconds), then
      write a compaction job and benchmark query time before/after.
- [ ] Exercise schema evolution, time travel, and retention/expire-snapshots.

**Deliverables:** Iceberg tables, three layers, partition strategy, compaction job, before/after benchmark.

### Phase 6 — Trino analytics (Week 4)

- [ ] Add Trino with the Iceberg connector.
- [ ] Write the failure-rate-by-software-version query.
- [ ] Write the pedestrian-detection-failure query joining perception, planning,
      and telemetry on `vehicle_id` + time window.
- [ ] Benchmark partition pruning and Parquet column pruning with `EXPLAIN ANALYZE`.
- [ ] Document lakehouse query design.

**Deliverables:** Trino deployment, analytics queries, query benchmarks, query-design doc.

**Milestone 1 exit criteria:** one command runs the simulator for 10 minutes and a
Trino query returns correlated perception, planning, and telemetry rows.

---

## 2. Milestone 2 — Data Integrity (Week 3, overlaps Milestone 1)

**Phases:** 4
**Goal:** no untyped JSON crosses the platform.

### Phase 4 — Schemas and data contracts

- [ ] Choose Avro + a schema registry (Confluent or Apicurio). Avro fits Kafka tooling
      best, and the Pydantic models from Phase 1 can generate the Avro schemas.
- [ ] Build `validation/` as a Kafka consumer that validates against the registry,
      dedups on `(vehicle_id, timestamp, event_type)`, and routes to
      `validated.*` or `quarantine.*` topics.
- [ ] Land quarantined records into a `quarantine` Iceberg table with a reason column.
- [ ] Add a `--bad-data-rate` flag to the simulator that injects: missing fields,
      invalid confidence, unknown object types, duplicates, old schema versions,
      corrupt timestamps.
- [ ] Emit quality metrics as Prometheus counters now (schema compliance, duplicate
      rate, invalid rate, late events, quarantined count) even though Grafana
      arrives in Milestone 5.
- [ ] Ship `PerceptionEvent v2` with one added optional field and prove v1 consumers still work.

**Deliverables:** versioned schemas, validation service, quarantine topic/table,
quality metrics, schema evolution example.

**Milestone 2 exit criteria:** 5% injected corruption lands in quarantine with
zero corrupt rows in validated tables.

---

## 3. Milestone 3 — Autonomy Intelligence (Weeks 5–6)

**Phases:** 7, 8
**Goal:** raw events become scenario records and versioned ML datasets.

### Phase 7 — Scenario mining (Week 5)

- [ ] Define scenario patterns declaratively: event sequence, time window, conditions.
- [ ] Implement first as a **batch** job over Iceberg in Python (faster to debug).
- [ ] Port the same rules to a PyFlink job using session windows keyed by
      `vehicle_id`, with checkpointing enabled.
- [ ] Write the `scenarios` table with: `scenario_id`, `vehicle_id`, `start_time`,
      `end_time`, `weather`, `software_version`, `objects`, `max_speed`, `braking`,
      `perception_confidence`, `outcome`.
- [ ] Produce a scenario statistics report.
- [ ] Confirm the firmware 3.2 correlation surfaces in discovered scenarios.

**Deliverables:** detection engine, temporal correlation logic, scenario table,
statistics, example discovered scenarios.

### Phase 8 — ML dataset pipeline (Weeks 5–6)

- [ ] Build `ml/datasets/` as a pipeline: filter → quality check → dedup → stratified sample.
- [ ] Each run writes a versioned Iceberg table plus a metadata JSON with example
      count, class distribution, source dates, software versions, environmental
      conditions, and diversity score.
- [ ] Produce `pedestrian_dataset_v1`, `_v2`, `_v3` using different sampling
      strategies (random, class-balanced, hard-negative mined).
- [ ] Write a dataset-quality report comparing the three versions.

**Deliverables:** dataset pipeline, versioned datasets, metadata, quality report.

**Milestone 3 exit criteria:** the request "give me 50,000 difficult pedestrian
scenarios" is answerable by one pipeline invocation.

---

## 4. Milestone 4 — ML/AI Platform (Weeks 6–8)

**Phases:** 9, 10, 11
**Goal:** trained model, semantic search, and a tool-using investigation assistant.

### Phase 9 — MLflow (Week 6)

- [ ] Add MLflow to compose, backed by Postgres (metadata) and MinIO (artifacts).
- [ ] Train a gradient-boosted classifier predicting perception/planning failure
      from: weather, speed, object_count, pedestrian_count, confidence, lighting,
      road_type, software_version.
- [ ] Log dataset version as a run tag so dataset → run → model lineage is queryable.
- [ ] Register the model and record evaluation metrics.

**Deliverables:** training pipeline, experiment tracking, model registry,
evaluation metrics, dataset → model lineage.

### Phase 10 — Semantic search (Week 7)

- [ ] Move current `app/` into `rag/`; restructure per Phase 14 layout.
- [ ] Write a scenario-to-text template
      (e.g. "Urban intersection at night. 28 mph. Three pedestrians. Heavy rain.
      Confidence dropped to 0.42. Emergency braking.").
- [ ] Embed with the existing Voyage provider; store in a `scenarios` Postgres
      table with a pgvector HNSW index.
- [ ] Adapt the hybrid retrieve from `rag.py` to scenario filters
      (weather, version, time range, outcome).
- [ ] Expose `/scenarios/search` in FastAPI.
- [ ] Build a small labeled query set and measure recall@10.

**Deliverables:** text generation, embedding pipeline, vector index,
search API, retrieval benchmark.

### Phase 11 — RAG investigation assistant (Weeks 7–8)

- [ ] Implement the five tools as functions over Trino and Postgres with
      parameterized, hard-coded SQL: `query_incidents`, `query_scenarios`,
      `search_documentation`, `get_vehicle_history`, `compare_software_versions`.
- [ ] Wire tools through Anthropic tool use so the model plans, calls tools, and
      cites results. No free-form SQL access.
- [ ] Return evidence-backed responses that reference tool outputs.
- [ ] Write 20 evaluation questions with expected evidence; score on citation accuracy.

**Deliverables:** RAG service, tool layer, prompt/tool definitions,
evidence-backed responses, evaluation dataset.

**Milestone 4 exit criteria:** "Why did pedestrian incidents increase after
version 4.7?" returns an answer citing specific tool outputs.

---

## 5. Milestone 5 — Production Platform (Weeks 8–10)

**Phases:** 12, 13, 14
**Goal:** observable, resilient, documented.

### Phase 12 — Lineage and observability (Weeks 8–9)

- [ ] Add Prometheus, Grafana, and OpenTelemetry to compose.
- [ ] Build four dashboards: pipeline (lag, events/sec, latency, failures, retries),
      data quality (freshness, completeness, duplicates, schema failures, late events),
      lakehouse (table size, file count, small files, query latency, compaction),
      AI (LLM latency, tokens, cost, retrieval quality, response quality).
- [ ] Emit OpenLineage events from each pipeline stage to Marquez
      (lighter than DataHub).
- [ ] Add three alerts: consumer lag, quarantine rate, freshness.

**Deliverables:** Grafana dashboards, alerts, data-quality dashboard,
AI observability dashboard, lineage visualization.

### Phase 13 — Chaos testing (Week 9)

Run each as a scripted, repeatable test under `infrastructure/chaos/`:

- [ ] Kill a Kafka broker — do we lose events?
- [ ] Kill a Flink task manager — does processing resume from checkpoint?
- [ ] Send events 10 minutes late — does scenario detection still work?
- [ ] Replay Kafka events — is the pipeline idempotent?
- [ ] Change the perception schema — do existing consumers keep working?
- [ ] Inject 5% corrupt records — do they poison downstream datasets?

For each, write a one-page report: failure mode, detection mechanism,
recovery mechanism, data-loss implications, operational tradeoffs.

### Phase 14 — Packaging (Week 10)

- [ ] Finalize repo layout and `README.md` with a fresh-clone walkthrough.
- [ ] Complete `architecture/architecture.md`, `data-model.md`, `decisions.md`, `benchmarks.md`.
- [ ] Write the ten ADRs: Kafka vs Kinesis, Iceberg, Parquet, Flink vs Spark
      Streaming, pgvector vs OpenSearch vs dedicated vector DB, batch + streaming,
      1K → 10K → 100K scaling, late-arriving data, idempotency, tenant isolation.
- [ ] Add Kubernetes manifests (Kompose or Helm) for core services, documented
      but not required for the local run.
- [ ] Add CI that runs the simulator, validator, and unit tests.
- [ ] Draft the resume description using only measured numbers.

**Milestone 5 exit criteria:** a fresh clone runs `docker compose up` and the
README walkthrough end to end.

---

## 6. Week-by-week schedule

| Week | Work |
|---|---|
| 1 | Phase 1; start Phase 2 |
| 2 | Finish Phase 2; start Phase 3 |
| 3 | Finish Phase 3; Phase 4; start Phase 5 |
| 4 | Finish Phase 5; Phase 6 |
| 5 | Phase 7; start Phase 8 |
| 6 | Finish Phase 8; Phase 9 |
| 7 | Phase 10; start Phase 11 |
| 8 | Finish Phase 11; start Phase 12 |
| 9 | Finish Phase 12; Phase 13 |
| 10 | Phase 14; buffer |

---

## 7. Risks and mitigations

- **Memory on a 16 GB laptop.** Kafka, Flink, MinIO, Trino, Postgres, MLflow,
  Grafana, and the API together exceed 16 GB at defaults. Use docker compose
  profiles per milestone (`--profile m1`, `--profile m3`, …) and cap JVM heaps
  for Kafka, Flink, and Trino at 1 GB each.
- **Flink is the hardest single piece.** Scenario mining is built in batch first
  so the week is not lost to PyFlink setup. If Flink slips, batch mining still
  satisfies Milestone 3 and Flink becomes a Milestone 5 stretch item.
- **Airflow is off the critical path.** Plain Python entrypoints and Makefile
  targets cover orchestration until Milestone 5. Add Airflow only if time remains.
- **Benchmarks must be recorded as you go**, not in Week 10. The resume section
  requires measured numbers only.
- **Scope creep from the "What Not to Build" list.** No video processing,
  no labeling platform, no custom vector DB, no custom LLM, no AWS until local works.

---

## 8. Technology choices locked in by this plan

| Concern | Choice | Reason |
|---|---|---|
| Kafka | Single-node KRaft | No ZooKeeper, lower memory |
| Schema | Avro + schema registry | Best Kafka tooling; generated from Pydantic |
| Iceberg catalog | REST catalog + PyIceberg | Simplest local setup; Trino-compatible |
| Vector store | Postgres + pgvector | Already in repo; saves ~2 GB vs OpenSearch |
| Embeddings | Voyage (existing provider) | Already wired in `providers.py` |
| LLM | Anthropic tool use (existing provider) | Already wired in `providers.py` |
| Lineage | OpenLineage → Marquez | Lighter than DataHub |
| Orchestration | Python entrypoints + Makefile | Airflow deferred to Milestone 5 |
| Streaming | Batch first, then PyFlink | De-risks the hardest component |
