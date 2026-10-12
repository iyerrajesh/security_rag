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
