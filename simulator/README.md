# Fleet simulator

Generates synthetic AV fleet events (telemetry, perception, planning, health,
software) that validate against the Pydantic models in [`schemas/`](../schemas).

```bash
simulator/run.sh --vehicles 1000 --events-per-second 10 --duration 3600 --seed 42
```

`run.sh` creates `.venv/` on first use and installs [`requirements.txt`](requirements.txt)
again only when that file changes. All flags go straight through to the simulator. To skip
the script, run `pip install -r simulator/requirements.txt` and then `python -m simulator ...`.

| Flag | Default | Meaning |
|---|---|---|
| `--vehicles` | 100 | Fleet size (`V1000`, `V1001`, …) |
| `--events-per-second` | 1 | Report rate **per vehicle**, in Hz |
| `--duration` | 60 | Simulated seconds |
| `--seed` | 42 | Same seed ⇒ byte-identical output |
| `--output` | `output/sim` | Output directory (git-ignored) |
| `--compress` | off | Write `.jsonl.gz` |
| `--sink` | `jsonl` | `jsonl` files, or `kafka` (see [ingestion/](../ingestion/README.md)) |
| `--bootstrap-servers` | `127.0.0.1:9094` | Kafka brokers, for `--sink kafka` |
| `--producer-config` | — | `KEY=VALUE` librdkafka setting, repeatable (e.g. `queue.buffering.max.messages=1000`) |
| `--start-time` | `2026-10-04T10:00:00Z` (jsonl), now (kafka) | Simulated clock start |
| `--scenario-rate` | 1.0 | Multiplier on scenario injection rates |
| `--faulty-share` | 0.3 | Fraction of the fleet starting on firmware 3.2 |
| `--realtime` | off | Pace output to wall-clock time instead of running flat out |

## Output

One JSONL file per Kafka topic, plus ground truth and run metadata:

| File | Contents | Rate |
|---|---|---|
| `telemetry.jsonl` | `TelemetryEvent` | every report tick |
| `planning.jsonl` | `PlanningEvent` | every report tick |
| `perception.jsonl` | `PerceptionEvent`, one per detected object | every report tick |
| `health.jsonl` | `HealthEvent` | 1 Hz |
| `software.jsonl` | `SoftwareEvent` | at boot and on each deployment |
| `scenarios.jsonl` | Ground-truth scenario episodes (not an event type) | — |
| `manifest.json` | Run config and event counts | — |

Events are written in timestamp order. `scenarios.jsonl` is the answer key for
scenario mining (Phase 7). Don't feed it into the pipeline. Episodes still
active when the run ends are closed at the end time.

A 100-vehicle, 60-second sample (seed 42, about 45k events) is in
[`sample_data/`](sample_data).

## How it works

Each vehicle is a state machine stepped at 10 Hz (physics), with events reported at
`--events-per-second`. Every tick runs:

1. **Scenarios.** Start or end episodes at random. Each vehicle has its own RNG seeded
   from `seed:vehicle_id`, so a vehicle behaves the same regardless of fleet size.
2. **World.** Objects move in the ego frame (`x` ahead, `y` left). Background
   traffic, pedestrians, cyclists, signs and cones spawn and despawn.
3. **Perception.** Confidence = base × distance × weather/night × camera state.
   Lidar confirms obstacles within 20 m. If the camera fails, lidar is the only
   source and objects are reported as `unknown`.
4. **Planner.** Only detections with confidence ≥ 0.5 are trusted. It picks a
   target speed from conditions and in-path hazards. If the deceleration needed
   to stop or follow exceeds 4.5 m/s², it brakes in an emergency (−8 m/s², or
   −6 m/s² in rain).
5. **Dynamics.** The planner's acceleration moves the vehicle, so telemetry,
   perception and planning stay consistent.

### Scenarios

| Scenario | Effect |
|---|---|
| normal driving | No active scenario; cruise at 11–16 m/s |
| `pedestrian_crossing` | Pedestrian crosses the lane 35–60 m ahead → yield/stop |
| `hard_braking` | Lead vehicle brakes at 7 m/s² → follow or emergency brake |
| `sensor_failure` | Camera or lidar `failed` for 10–40 s; speed capped at 5 m/s |
| `network_degradation` | `network_latency` of 300–1500 ms |
| `rain` | Confidence ×0.85, speed ×0.8, max decel 6 m/s² |
| `night` | Vulnerable-road-user confidence ×0.8 |
| `construction_zone` | Cones on the shoulder, lane change, speed capped at 8 m/s |
| `software_deployment` | Upgrades 4.6 / fw 3.1 → 4.7 / fw 3.2 and emits a `SoftwareEvent` |

### Planted failure chain

```text
firmware 3.2 → camera degradation episodes → lower pedestrian confidence → emergency braking
```

Firmware 3.2 starts camera-degradation episodes at 25× the baseline rate.
A degraded camera cuts confidence by 40%. Pedestrians then stay below the trust
threshold until lidar confirms them at about 20 m, which is too late for a
comfortable stop. In practice, vehicles on 3.2 brake in an emergency about 3–4×
as often as those on 3.1. Hard braking by a lead vehicle causes emergency
braking on any firmware, which acts as a confounder.

All tunables are in [`config.py`](config.py).

## Tests

```bash
pip install pytest
python -m pytest tests/test_simulator.py
```

The tests check:
- every event passes schema validation
- output is reproducible from the seed
- events are reported at the configured rates
- every scenario occurs
- telemetry and planning are physically consistent
- the failure chain is present in the data
