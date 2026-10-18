# Validation and data contracts (Phase 4)

No arbitrary JSON flows past this point. Raw fleet events are checked against
the registered schema for their declared version. Valid ones are re-published
as Avro, and everything else is quarantined with a reason.

```text
vehicle.<type>  (raw JSON, untrusted)
      │
      ▼
  validator ──── valid, first time seen ───► validated.vehicle.<type>  (Avro, schema registry)
      │    └──── duplicate event_id ───────► dropped (counted)
      └───────── fails its contract ───────► quarantine.vehicle  (original bytes + reason headers)
```

## Quick start

```bash
docker compose --profile m1 up -d kafka schema-registry kafka-init kafka-ui
.venv/bin/python -m validation.registry register          # once; idempotent
simulator/run.sh --sink kafka --bad-data-rate 0.05        # produce with 5% bad data
.venv/bin/python -m validation                            # run the validator (Ctrl-C to stop)
.venv/bin/python -m validation.inspect quarantine         # what was rejected, and why
validation/e2e.sh                                         # full repeatable test (wipes local topics)
```

| Tool | Purpose |
|---|---|
| `python -m validation` | The validator service. `--idle-timeout N` exits when idle; `--summary-json` writes totals |
| `python -m validation.registry register \| list \| evolution` | Register schemas, list contracts, show compatibility decisions |
| `python -m validation.inspect validated \| quarantine` | Decode output topics; `--perception-reader 1` reads as a v1 consumer would |
| `python -m schemas.avro [--check]` | Regenerate the committed Avro schemas from the Pydantic models |

Schema Registry runs at http://localhost:8081, and Kafka UI (http://localhost:8080)
decodes the Avro topics with it.

## Contracts

The Pydantic models in [`schemas/`](../schemas) are the source of truth.
`python -m schemas.avro` generates one Avro schema per event type and version
into [`schemas/avro/`](../schemas/avro), and these files are committed. A test
fails if they drift from the models. They are registered under
`validated.vehicle.<type>-value` with **FULL** compatibility.

Avro checks structure: field names, types, enums, and which fields are required.
It can't check value ranges or string patterns such as confidence 0–1 or
`vehicle_id` matching `V\d+`. The validator therefore applies both:

1. **Structure, from the registry.** The record's fields must exactly match the
   registered schema for the `schema_version` it declares.
2. **Semantics, from Pydantic.** Ranges, patterns, enum values, and timestamps
   that include a timezone.

Contracts are re-read from the registry every 30 s, so a newly registered
version is accepted without restarting the validator.

### Quarantine reasons

Checks run in this order; the first one that fails gives the reason.

| Reason | Meaning |
|---|---|
| `malformed_json` | Not parseable, or not a JSON object |
| `wrong_event_type` | `event_type` doesn't match the topic |
| `unsupported_schema_version` | `schema_version` isn't registered for this type |
| `missing_field` | A field the version requires is absent |
| `unexpected_field` | A field that isn't in that version (for example `occlusion` on a v1 record) |
| `invalid_timestamp` | Unparseable or without a timezone |
| `unknown_enum` | Value not in the enum, such as `object_type` |
| `invalid_value` | Out of range or doesn't match the pattern |

Quarantined messages keep the **original bytes** as their value, for
investigation and replay. Their headers are `reason`, `detail`, `source_topic`,
`source_partition` and `source_offset`. The quarantine topic keeps data for 30
days, against 7 for the others. `python -m lakehouse sink quarantine` lands them in the
`quarantine.vehicle` Iceberg table (see [`lakehouse/README.md`](../lakehouse/README.md)).

### Duplicates and late events

- **Duplicates** are dropped based on `event_id`. The plan suggested
  `(vehicle_id, timestamp, event_type)`, but perception emits several objects
  with the same timestamp, so that key would drop real detections. Dedup state
  is in memory and capped at 2M ids. After a restart, duplicates must be caught
  by idempotent writes downstream.
- **Late events** are more than 10 s (`--lateness`) older than the newest event
  of the same vehicle and type. They are still published, with a `late: true`
  header, and counted. Lateness is measured in event time, so it doesn't depend
  on how fast the simulator runs.

## Schema evolution: PerceptionEvent v2

v2 adds an optional `occlusion` field (0–1). Perception stack **2.1.0** emits v2,
while vehicles on 2.0.3 keep emitting v1, so the fleet sends both at once, as a
real fleet mid-rollout would.

| Change | Registry (FULL) |
|---|---|
| v2: add optional `occlusion` | compatible |
| add an optional field with a default | compatible |
| add a **required** field | rejected |
| remove a required field | rejected |

`python -m validation.registry evolution` asks the registry about each of these.

To add a field:
1. Add it to the Pydantic model as optional, with `json_schema_extra=since(N)`.
2. Bump the model's `schema_version` default.
3. Run `python -m schemas.avro`.
4. Run `python -m validation.registry register`.

On the validated topics:
- **Writing:** the validator always writes with the latest schema, so v1 records
  are upcast with `occlusion = null`.
- **Reading:** Avro schema resolution means consumers on either version keep
  working. A **v1 consumer** decodes v2 data and simply doesn't see
  `occlusion`, which is proven in the e2e test and in `tests/test_schemas.py`.

## Metrics

Prometheus metrics are served at `http://127.0.0.1:9108/metrics`. Grafana arrives
in Milestone 5.

| Metric | Labels |
|---|---|
| `dq_events_total` | `event_type`, `outcome` (valid, quarantined, duplicate) |
| `dq_quarantined_total` | `event_type`, `reason` |
| `dq_late_events_total` | `event_type` |
| `dq_schema_version_total` | `event_type`, `version` |

Useful queries:
- **Schema compliance:**
  `sum(rate(dq_events_total{outcome="valid"}[5m])) / sum(rate(dq_events_total{outcome!="duplicate"}[5m]))`
- **Duplicate rate:** `outcome="duplicate"` over the total
- **v2 rollout progress:** `dq_schema_version_total{event_type="perception"}`, by version

## Delivery guarantees

The validator is at-least-once:
1. It consumes with auto-commit off.
2. Every second, it flushes the producer and waits until every validated and
   quarantined record has been acknowledged.
3. Only then does it commit the input offsets.

A crash therefore re-processes up to about a second of input instead of losing
it. Re-processed records can be published twice, which is the same at-least-once
contract the chaos tests showed for Kafka consumers.

## Results (`validation/e2e.sh`, 100 vehicles × 60 s, 5% bad data)

| Injected | Count | Validator outcome | Count |
|---|---:|---|---:|
| corrupt_payload | 334 | `malformed_json` | 334 |
| corrupt_timestamp | 333 | `invalid_timestamp` | 333 |
| missing_field | 328 | `missing_field` | 328 |
| invalid_confidence | 166 | `invalid_value` | 166 |
| unknown_object_type | 170 | `unknown_enum` | 170 |
| old_schema_version | 367 | `unsupported_schema_version` | 367 |
| duplicate | 339 | dropped as duplicate | 339 |
| late | 314 | published, flagged late | 314 |

43,508 valid records were published, with no clean record rejected. Perception
split 13,620 v1 / 12,398 v2. The validator processed about 19k records/s.
