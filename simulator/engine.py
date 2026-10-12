"""Simulation loop: advances every vehicle in lockstep and streams events to a sink.

Physics runs at a fixed small step; events are reported at `events_per_second`
per vehicle (telemetry + planning + one perception event per detected object),
health at 1 Hz. Events are written in timestamp order.
"""

import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import config as C
from .sinks import Sink
from .vehicle import Vehicle

DEFAULT_START = datetime(2026, 10, 4, 10, 0, tzinfo=timezone.utc)


@dataclass
class SimulationConfig:
    vehicles: int = 100
    events_per_second: float = 1.0
    duration: float = 60.0
    seed: int = 42
    start_time: datetime = field(default=DEFAULT_START)
    scenario_rate: float = 1.0
    faulty_share: float = C.INITIAL_FAULTY_SHARE
    realtime: bool = False


def run(cfg: SimulationConfig, sink: Sink) -> Counter:
    dt = min(C.PHYSICS_DT, 1.0 / cfg.events_per_second)
    report_every = max(1, round(1.0 / (cfg.events_per_second * dt)))
    health_every = max(1, round(C.HEALTH_INTERVAL / dt))
    steps = round(cfg.duration / dt)

    fleet = [Vehicle(f"V{1000 + i}", cfg.seed, cfg.start_time, cfg.faulty_share, cfg.scenario_rate)
             for i in range(cfg.vehicles)]
    counts: Counter = Counter()

    def emit(events) -> None:
        for event in events:
            sink.write(event)
            counts[event.event_type] += 1

    for vehicle in fleet:
        emit(vehicle.boot_events())

    wall_start = time.monotonic()
    for step in range(steps):
        t = step * dt
        report = step % report_every == 0
        report_health = step % health_every == 0
        for vehicle in fleet:
            emit(vehicle.step(t, dt, report, report_health))
        if cfg.realtime:
            time.sleep(max(0.0, wall_start + (step + 1) * dt - time.monotonic()))

    for vehicle in fleet:
        for episode in sorted(vehicle.close(steps * dt), key=lambda e: e.start_s):
            sink.write_label(episode.to_record(cfg.start_time))
            counts[f"scenario:{episode.scenario.value}"] += 1
    return counts
