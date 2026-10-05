# Autonomous Systems Data & AI Platform
## Personal Portfolio Project — 8–10 Week Implementation Plan

## 1. Project Goal

Build a realistic, end-to-end **Autonomous Vehicle / Robotics Data & AI Platform** that manages the lifecycle of telemetry, perception, planning, system-health, and software-deployment data from a simulated fleet.

The platform should support:

- High-volume event ingestion
- Data contracts and validation
- Data-quality enforcement
- A modern lakehouse
- Batch and streaming analytics
- Temporal event correlation
- Scenario mining
- ML training-data generation
- ML experiment/model tracking
- Semantic scenario search
- RAG and AI-assisted investigation
- Data lineage
- Pipeline and AI observability
- Failure recovery and scalability testing

The project is intentionally designed so the AI layer is **downstream of a substantial data platform**, rather than being a simple chatbot application.

---

# 2. Target Architecture

```text
                    Synthetic AV Fleet
                           │
                           ▼
                    Edge Data Generator
                           │
                    Kafka / MQTT
                           │
                           ▼
                 ┌──────────────────┐
                 │ Ingestion Layer  │
                 │ Schema Validation│
                 │ Data Quality     │
                 └────────┬─────────┘
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
       Raw S3 / MinIO             Kafka/Flink
             │                         │
             ▼                         ▼
      Iceberg Lakehouse         Real-time Events
             │                         │
             └────────────┬────────────┘
                          ▼
                   Curated Datasets
                          │
             ┌────────────┼─────────────┐
             ▼            ▼             ▼
          Trino       PostgreSQL       MLflow
             │            │             │
             ▼            ▼             ▼
       Analytics     Operational     ML Pipeline
                                     │
                                     ▼
                              Training Datasets
                                     │
                                     ▼
                               Scenario Search
                                     │
                         ┌───────────┴──────────┐
                         ▼                      ▼
                    Vector DB                 RAG
                         │                      │
                         └──────────┬───────────┘
                                    ▼
                              AI Assistant
```

---

# 3. Project Roadmap

The project is divided into five major milestones:

### Milestone 1 — Data Platform

> Simulator → Kafka → S3/Iceberg → Trino

### Milestone 2 — Data Integrity

> Schemas → validation → quarantine → quality metrics

### Milestone 3 — Autonomy Intelligence

> Flink → temporal joins → scenario mining → ML datasets

### Milestone 4 — ML/AI Platform

> MLflow → embeddings → semantic scenario search → RAG

### Milestone 5 — Production Platform

> Kubernetes → observability → lineage → failure recovery → load testing

The first three milestones already constitute a strong data-engineering project. Milestones 4–5 extend it into modern AI/data-platform engineering.

---

# 4. Phase 1 — Define the Autonomous-System Data Model

**Target: Week 1**

Before implementing Kafka or AI, define the data produced by the simulated fleet.

Create five core event types.

## 4.1 Vehicle Telemetry

Fields:

```text
vehicle_id
timestamp
speed
acceleration
braking
steering
battery
gps_lat
gps_lon
```

## 4.2 Perception

Fields:

```text
vehicle_id
timestamp
object_id
object_type
confidence
x
y
velocity
```

Example:

```json
{
  "vehicle_id": "V1023",
  "timestamp": "2026-10-04T10:30:15Z",
  "object_id": "P832",
  "object_type": "pedestrian",
  "confidence": 0.71
}
```

## 4.3 Planning

Fields:

```text
vehicle_id
timestamp
planned_action
target_speed
emergency_braking
trajectory_id
```

## 4.4 System Health

Fields:

```text
vehicle_id
timestamp
cpu
memory
gpu
camera_status
lidar_status
network_latency
```

## 4.5 Software / Deployment Events

Fields:

```text
vehicle_id
timestamp
software_version
perception_version
firmware_version
deployment_id
```

### Deliverables

- Data model document
- Event schemas
- Example JSON records
- Initial architecture diagram

---

# 5. Phase 2 — Build the Fleet Simulator

**Target: Week 1–2**

Build a Python simulator that generates realistic events.

Start with:

```text
100 vehicles
```

Then:

```text
1,000 vehicles
```

Eventually demonstrate:

```text
10,000 vehicles
```

You do not need to run 10,000 vehicles continuously on your laptop.

Make the simulator configurable:

```bash
python simulator.py \
  --vehicles 1000 \
  --events-per-second 10 \
  --duration 3600
```

## Generate Realistic Scenarios

Include scenarios such as:

```text
Normal driving
Pedestrian crossing
Hard braking
Sensor failure
Network degradation
Rain
Night
Construction zone
Software deployment
```

Most importantly, introduce **correlated failures**.

Example:

```text
Firmware 3.2
     ↓
Camera degradation
     ↓
Lower pedestrian confidence
     ↓
Emergency braking
```

This correlation will later become something your platform should discover.

### Deliverables

- Python simulator
- Configurable vehicle/event rates
- Scenario generator
- Reproducible random seed
- Sample datasets

---

# 6. Phase 3 — Build the Ingestion Platform

**Target: Week 2–3**

Introduce Kafka.

Create topics:

```text
vehicle.telemetry
vehicle.perception
vehicle.planning
vehicle.health
vehicle.software
```

Partition by:

```text
vehicle_id
```

Study and document:

- Partitioning
- Ordering
- Consumer groups
- Replay
- Retention
- Throughput
- Backpressure

### Deliverables

- Kafka deployment
- Producers
- Consumers
- Topic configuration
- Throughput benchmark
- Kafka architecture documentation

---

# 7. Phase 4 — Add Schema and Data Contracts

**Target: Week 3**

Do not allow arbitrary JSON to flow through the platform.

Use:

- Avro, or
- Protobuf

Example perception schema:

```text
PerceptionEvent v1

vehicle_id       string
timestamp        timestamp
object_id        string
object_type      enum
confidence       float
x                float
y                float
```

## Deliberately Inject Bad Data

Introduce:

- Missing fields
- Invalid confidence values
- Unknown object types
- Duplicate events
- Old schema versions
- Corrupt timestamps

Build:

```text
Kafka
  │
  ▼
Schema validation
  │
  ├── Valid ──→ Processing
  │
  └── Invalid → Quarantine
```

Track:

```text
Schema compliance
Duplicate rate
Invalid event rate
Late events
Quarantined records
```

### Deliverables

- Versioned schemas
- Schema validation service
- Quarantine topic/table
- Data-quality metrics
- Schema evolution example

---

# 8. Phase 5 — Build the Lakehouse

**Target: Week 3–4**

Introduce:

- MinIO or S3
- Apache Iceberg
- Parquet

Architecture:

```text
Kafka
  │
  ▼
Ingestion
  │
  ▼
S3 / MinIO
  │
  ▼
Iceberg
```

Create logical layers:

```text
raw/
validated/
curated/
```

Example tables:

```text
raw_perception
validated_perception
vehicle_telemetry
vehicle_health
software_deployments
```

## Study and Implement

- Partitioning
- Schema evolution
- ACID transactions
- Time travel
- Incremental processing
- Compaction
- Retention
- Small-file management

### Important Exercise

Deliberately create a small-file problem and solve it with compaction.

This becomes useful interview material.

### Deliverables

- Iceberg tables
- Raw/validated/curated layers
- Partition strategy
- Compaction process
- Benchmark before/after compaction

---

# 9. Phase 6 — Query with Trino

**Target: Week 4**

Build analytics queries over the lakehouse.

Examples:

## Failure Rate by Software Version

```sql
SELECT
    software_version,
    COUNT(*) AS incidents
FROM incidents
GROUP BY software_version;
```

## Pedestrian Detection Failures

Correlate:

```text
Perception
    JOIN
Planning
    JOIN
Telemetry
```

using:

```text
vehicle_id
+
time window
```

Benchmark:

- Partition pruning
- Parquet column pruning
- Temporal joins
- Pre-aggregations
- Query latency

### Deliverables

- Trino deployment
- Analytics queries
- Query-performance benchmarks
- Documentation of lakehouse query design

---

# 10. Phase 7 — Build Scenario Mining

**Target: Week 5**

This is the heart of the project.

Define a **scenario** as a correlated sequence of events.

Example:

```text
T0
Pedestrian detected

T0 + 1 sec
Confidence falls

T0 + 2 sec
Vehicle brakes

T0 + 3 sec
Planning changes trajectory
```

The pipeline identifies:

```text
Scenario:
"Low-confidence pedestrian detection followed by
emergency braking"
```

Create a scenario record:

```text
scenario_id
vehicle_id
start_time
end_time
weather
software_version
objects
max_speed
braking
perception_confidence
outcome
```

This transforms raw events into a higher-level representation useful to engineers and ML teams.

### Deliverables

- Scenario detection engine
- Temporal correlation logic
- Scenario table
- Scenario statistics
- Example discovered scenarios

---

# 11. Phase 8 — Build the ML Dataset Pipeline

**Target: Week 5–6**

Simulate an ML team's request:

> "Give me 50,000 difficult pedestrian scenarios."

Build:

```text
Raw data
    ↓
Scenario mining
    ↓
Filtering
    ↓
Quality checks
    ↓
Deduplication
    ↓
Sampling
    ↓
Dataset v1
```

Create dataset versions:

```text
pedestrian_dataset_v1
pedestrian_dataset_v2
pedestrian_dataset_v3
```

Track:

- Number of examples
- Class distribution
- Source dates
- Software versions
- Environmental conditions
- Scenario diversity

Implement concepts such as:

- Hard-negative mining
- Class balancing
- Stratified sampling
- Scenario diversity
- Dataset versioning

### Deliverables

- Dataset-generation pipeline
- Versioned datasets
- Dataset metadata
- Dataset-quality report

---

# 12. Phase 9 — Add MLflow

**Target: Week 6**

Train a simple model.

Example objective:

> Predict whether a scenario will result in a perception/planning failure.

Possible features:

```text
weather
speed
object_count
pedestrian_count
confidence
lighting
road_type
software_version
```

Use MLflow for:

```text
experiments
models
parameters
metrics
dataset versions
model versions
```

Track:

```text
Dataset v17
     ↓
Training run 42
     ↓
Model v3
     ↓
Evaluation
     ↓
Production candidate
```

### Deliverables

- Training pipeline
- MLflow experiment tracking
- Model registry
- Evaluation metrics
- Dataset → model lineage

---

# 13. Phase 10 — Add Semantic Search

**Target: Week 7**

Now introduce the AI portion.

Generate a textual representation of each scenario.

Example:

> Urban intersection at night. Vehicle traveling 28 mph. Three pedestrians detected. Heavy rain. Pedestrian confidence dropped to 0.42. Emergency braking occurred.

Generate embeddings.

Store them in:

- OpenSearch, or
- PostgreSQL + pgvector

OpenSearch is particularly useful because it connects naturally to an existing Elasticsearch/search background.

Now support queries such as:

> "nighttime pedestrian failures during rain"

The system should retrieve semantically similar scenarios.

### Deliverables

- Scenario text-generation pipeline
- Embedding pipeline
- Vector index
- Semantic search API
- Retrieval-quality benchmark

---

# 14. Phase 11 — Build the RAG Investigation Assistant

**Target: Week 7–8**

Introduce an LLM.

Do not give the LLM unrestricted database access.

Instead, provide controlled tools:

```text
query_incidents()
query_scenarios()
search_documentation()
get_vehicle_history()
compare_software_versions()
```

Example user query:

> "Why did pedestrian-related incidents increase after version 4.7?"

The AI system can execute:

```text
1. Compare incident rates
2. Group by software version
3. Filter pedestrian incidents
4. Compare environmental conditions
5. Search deployment documentation
6. Retrieve similar scenarios
```

Then generate an evidence-backed explanation.

The goal is an **AI investigation assistant**, not a generic chatbot.

### Deliverables

- RAG service
- Tool/API layer
- Prompt/tool definitions
- Evidence-backed responses
- Evaluation dataset

---

# 15. Phase 12 — Add Data Lineage and Observability

**Target: Week 8–9**

Make the platform production-like.

## Pipeline Metrics

Track:

```text
Kafka lag
Events/sec
Processing latency
Failed events
Retries
```

## Data Quality

Track:

```text
Freshness
Completeness
Duplicates
Schema failures
Late events
```

## Lakehouse

Track:

```text
Table size
File count
Small files
Query latency
Compaction frequency
```

## AI

Track:

```text
LLM latency
Token usage
Cost
Retrieval quality
Response quality
```

Use:

- Prometheus
- Grafana
- OpenTelemetry

For lineage, evaluate:

- OpenLineage
- DataHub

### Deliverables

- Grafana dashboards
- Pipeline alerts
- Data-quality dashboard
- AI observability dashboard
- Lineage visualization

---

# 16. Phase 13 — Break It Deliberately

**Target: Week 9**

Introduce failures and document system behavior.

## Kafka Failure

Kill a broker.

Question:

> Do we lose events?

## Flink Failure

Kill a task manager.

Question:

> Does processing resume from checkpoint?

## Late Data

Send events 10 minutes late.

Question:

> Does scenario detection still work?

## Duplicate Data

Replay Kafka events.

Question:

> Is the pipeline idempotent?

## Schema Evolution

Change the perception schema.

Question:

> Do existing consumers continue working?

## Bad Data

Inject 5% corrupt records.

Question:

> Do they poison downstream datasets?

Document:

- Failure mode
- Detection mechanism
- Recovery mechanism
- Data-loss implications
- Operational tradeoffs

These become strong distributed-systems interview stories.

---

# 17. Phase 14 — Package the Project

**Target: Week 10**

Recommended repository structure:

```text
autonomy-data-platform/
│
├── simulator/
├── schemas/
├── ingestion/
├── validation/
├── streaming/
├── lakehouse/
├── scenario-mining/
├── ml/
├── embeddings/
├── rag/
├── api/
├── infrastructure/
├── dashboards/
│
├── docker-compose.yml
├── README.md
│
└── architecture/
    ├── architecture.md
    ├── data-model.md
    ├── decisions.md
    └── benchmarks.md
```

## Architecture Decision Records

Document decisions such as:

- Why Kafka instead of Kinesis?
- Why Iceberg?
- Why Parquet?
- Why Flink instead of Spark Streaming?
- Why OpenSearch instead of a dedicated vector database?
- Why batch + streaming?
- How would this scale from 1K → 10K → 100K vehicles?
- How do you handle late-arriving data?
- How do you guarantee idempotency?
- How do you enforce tenant/data isolation?

These design documents are as valuable for interviews as the code.

---

# 18. Recommended Technology Stack

| Layer | Technology |
|---|---|
| Language | Python |
| Messaging | Kafka |
| Schema | Protobuf / Avro |
| Streaming | Flink |
| Object Storage | S3 / MinIO |
| Table Format | Apache Iceberg |
| File Format | Parquet |
| Query | Trino |
| Operational DB | PostgreSQL |
| Search | OpenSearch |
| ML Lifecycle | MLflow |
| AI | LLM API |
| Embeddings | OpenAI or open-source |
| Orchestration | Airflow |
| API | FastAPI |
| Containers | Docker |
| Infrastructure | Kubernetes |
| Observability | Prometheus / Grafana |
| Lineage | OpenLineage / DataHub |

Do **not** install everything on day one. Add technologies incrementally as each phase requires them.

---

# 19. What Not to Build Initially

## Don't start with video processing

Avoid:

```text
Camera → YOLO → Video → GPU → ...
```

It adds substantial complexity without improving the core data-platform story.

Represent video/sensor information initially through metadata and synthetic scenario descriptors.

A small amount of real video can be added later if useful.

## Don't build a labeling platform

Use a simple UI or simulated labeling workflow.

## Don't build your own vector database

Use OpenSearch or pgvector.

## Don't build your own LLM

Use an LLM API.

## Don't deploy everything to AWS initially

Get the complete architecture working locally first.

---

# 20. Running It Locally

Start with Docker Compose.

The initial local stack can be:

```text
Kafka
Flink
MinIO
Iceberg
Trino
PostgreSQL
OpenSearch
MLflow
FastAPI
Grafana
```

Given a 16 GB MacBook, keep the initial scale modest.

Start with:

```text
100 vehicles
10 events/sec
```

Then increase toward:

```text
1,000 vehicles
```

Use synthetic load tests to demonstrate how the architecture would scale to:

```text
10,000 vehicles
100,000+ events/sec
```

You do not need to physically run the largest scale locally.

---

# 21. Final Resume Version

After the project is actually implemented and benchmarked, a resume description could look like:

> **Autonomous Systems Data & AI Platform — Personal Project**
>
> Designed and built a cloud-native data platform for managing autonomous-vehicle telemetry, perception, planning, and incident data across a simulated fleet. Implemented Kafka-based ingestion, schema/data-quality enforcement, an S3/Iceberg lakehouse, and Spark/Flink pipelines for temporal event correlation and scenario mining.
>
> Built automated ML dataset generation and scenario-selection pipelines with dataset/model lineage using MLflow, enabling identification and labeling of high-value edge cases for model improvement.
>
> Developed an AI-powered investigation layer combining structured SQL analytics, semantic scenario search, and RAG to investigate perception failures and identify correlations across software releases, environmental conditions, and vehicle behavior.

Only include quantitative scale or performance numbers once you have actually measured them.

---

# 22. Career Value

This project is intentionally aligned with a **Data Platform / Engineering Manager / Principal Engineer** profile.

It demonstrates:

### Distributed Systems

- Kafka
- Flink
- Event ordering
- Partitioning
- Backpressure
- Checkpointing
- Failure recovery

### Data Engineering

- S3
- Iceberg
- Parquet
- Spark/Flink
- Trino
- Batch + streaming

### Data Quality

- Data contracts
- Schema evolution
- Validation
- Freshness
- Completeness
- Deduplication
- Lineage

### ML Platform

- Training datasets
- Dataset versioning
- Feature generation
- MLflow
- Model evaluation
- Model lineage

### AI

- Embeddings
- Vector search
- Semantic retrieval
- RAG
- Tool-using LLMs
- AI evaluation

### Platform Engineering

- Kubernetes
- APIs
- CI/CD
- Observability
- Reliability
- Scalability

---

# 23. The Core Narrative

The project should ultimately support this career narrative:

> **"I've spent my career building distributed data and infrastructure platforms. I extended that experience into modern AI/ML data infrastructure, building the systems that make data reliable, discoverable, and usable for AI at scale."**

That positioning is more credible for an Engineering Manager / Principal Engineer / Data Platform Lead than positioning yourself as an AI application engineer.
