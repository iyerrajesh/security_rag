# Data Model

The simulated fleet produces five event types. Each one is defined once as a
Pydantic model in [`schemas/`](../schemas). The simulator, the validator, and
(later) the Iceberg table definitions all derive from those models, so this
document describes them; the code is the source of truth.

## Common envelope

Every event carries these fields (`schemas/base.py`):

| Field | Type | Notes |
|---|---|---|
| `event_type` | string literal | `telemetry` \| `perception` \| `planning` \| `health` \| `software`. Selects the model when parsing. |
| `schema_version` | int ≥ 1 | Version of this event type's schema. Perception is at `2`; every other type is at `1`. See *Versions*. |
| `event_id` | UUID | Unique per event. Used for deduplication and idempotent writes. |
| `vehicle_id` | string, `^V\d+$` | e.g. `V1023`. Kafka partition key and Iceberg bucket key. |
| `timestamp` | timestamp, UTC | Event time. Must include a timezone; normalized to UTC. |

Validation rules for all types:
- unknown fields are rejected (`extra="forbid"`)
- models are immutable
- strings are trimmed of surrounding whitespace

`(vehicle_id, timestamp)` is the join key for temporal correlation across event types.

## Event types

### Telemetry → `vehicle.telemetry`

Vehicle motion and position. Reported on every report tick.

| Field | Type | Unit / range |
|---|---|---|
| `speed` | float | m/s, ≥ 0 |
| `acceleration` | float | m/s², longitudinal; negative = decelerating |
| `braking` | float | 0 (none) – 1 (full brake pressure) |
| `steering` | float | degrees, −540 – 540; negative = left |
| `battery` | float | percent, 0 – 100 |
| `gps_lat` | float | degrees, −90 – 90 |
| `gps_lon` | float | degrees, −180 – 180 |

### Perception → `vehicle.perception`

One event per detected object per report tick.

| Field | Type | Unit / range |
|---|---|---|
| `object_id` | string | Track id, stable across ticks for one vehicle (e.g. `P832`) |
| `object_type` | enum | `pedestrian`, `vehicle`, `cyclist`, `motorcycle`, `traffic_cone`, `traffic_sign`, `animal`, `unknown` |
| `confidence` | float | 0 – 1 |
| `x` | float | metres ahead of the vehicle |
| `y` | float | metres to the left of the vehicle |
| `velocity` | float, optional | object speed, m/s |
| `occlusion` | float, optional, **v2+** | fraction of the object hidden from the camera, 0 – 1 |

### Planning → `vehicle.planning`

The planner's decision. Reported on every report tick.

| Field | Type | Unit / range |
|---|---|---|
| `planned_action` | enum | `cruise`, `accelerate`, `decelerate`, `stop`, `yield`, `lane_change_left`, `lane_change_right`, `turn_left`, `turn_right` |
| `target_speed` | float | m/s, ≥ 0 |
| `emergency_braking` | bool | |
| `trajectory_id` | string | Changes whenever the planned action changes |

### System health → `vehicle.health`

Compute and sensor health, 1 Hz.

| Field | Type | Unit / range |
|---|---|---|
| `cpu`, `memory`, `gpu` | float | percent utilization, 0 – 100 |
| `camera_status`, `lidar_status` | enum | `ok`, `degraded`, `failed`, `offline` |
| `network_latency` | float | ms, ≥ 0 |

### Software / deployment → `vehicle.software`

Installed versions. Emitted when the vehicle boots and on every deployment.

| Field | Type | Unit / range |
|---|---|---|
| `software_version` | string | `^\d+(\.\d+){0,2}$`, e.g. `4.7` |
| `perception_version` | string | same pattern, e.g. `2.1.0` |
| `firmware_version` | string | same pattern, e.g. `3.2` |
| `deployment_id` | string | Identifies the rollout |

To get the versions running at time *t*, take the vehicle's latest software
event at or before *t* (an as-of join).

## Examples

There is one valid record per type in
[`schemas/examples/`](../schemas/examples). Together they describe a single
incident on `V1023`:
1. Firmware 3.2 was deployed.
2. Later the camera degraded and a pedestrian was detected at 0.71 confidence.
3. The vehicle made an emergency stop.

## Parsing and routing

```python
from schemas import parse_event, topic_for

event = parse_event(raw_json)   # concrete model chosen by event_type; raises ValidationError
topic = topic_for(event)        # e.g. "vehicle.perception"
```

## Versions and Avro

Each event type is versioned with `schema_version`. A field added after v1 is
marked `json_schema_extra=since(N)` and must be optional. Records are sent in
their own version's shape: a v1 record omits v2 fields rather than sending them
as null.

| Event type | Versions | Changes |
|---|---|---|
| perception | 1, 2 | v2 adds optional `occlusion`. Perception stack 2.1.0+ sends v2 |
| telemetry, planning, health, software | 1 | |

`python -m schemas.avro` generates an Avro schema for every type and version
into [`schemas/avro/`](../schemas/avro). The registry stores them under
`validated.vehicle.<type>-value` with FULL compatibility. See
[`validation/README.md`](../validation/README.md) for how records are checked
against them.

## Iceberg tables

[`lakehouse/tables.py`](../lakehouse/tables.py) generates each typed table's schema from these models
(`Optional` fields become optional columns), so a model change is a single edit. Typed tables add `late`,
`kafka_partition`, `kafka_offset` and `ingest_time`; curated tables add `curated_time`. Every table is partitioned
by `day(timestamp)` and `bucket(8, vehicle_id)`. Layers and delivery semantics are in
[`lakehouse/README.md`](../lakehouse/README.md). Adding a field to a model means appending a column to the tables
with `update_schema().add_column(...)`: see the `evolve` demo (`python -m lakehouse demo evolve`).

## Ground truth (simulator only)

The simulator also writes `scenarios.jsonl`: one record per injected scenario
episode, with `vehicle_id`, `scenario`, `start`, `end` and `details`. This
record is **not** an event type and never flows through the pipeline. It is the
answer key for evaluating scenario mining (Phase 7). See
[`simulator/README.md`](../simulator/README.md).

## Known gaps / planned changes

- **No weather or lighting field.** Rain and night are visible only through
  their effects (lower confidence, lower speed) and in the ground truth. The
  Phase 7 scenario record needs `weather`, so we must decide whether to infer it
  or add it to telemetry.
