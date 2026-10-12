import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from . import config as C
from .engine import DEFAULT_START, SimulationConfig, run
from .sinks import JsonlSink


def _positive(kind):
    def parse(value: str):
        v = kind(value)
        if v <= 0:
            raise argparse.ArgumentTypeError("must be > 0")
        return v
    return parse


def _utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError("start time must include a timezone, e.g. 2026-10-04T10:00:00Z")
    return dt


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="python -m simulator", description="Synthetic AV fleet event generator.")
    p.add_argument("--vehicles", type=_positive(int), default=100)
    p.add_argument("--events-per-second", type=_positive(float), default=1.0,
                   help="report rate per vehicle, in Hz (default: 1)")
    p.add_argument("--duration", type=_positive(float), default=60.0, help="simulated seconds (default: 60)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output", type=Path, default=Path("output/sim"), help="output directory")
    p.add_argument("--compress", action="store_true", help="write .jsonl.gz")
    p.add_argument("--start-time", type=_utc, default=DEFAULT_START, help="sim start, ISO 8601 with timezone")
    p.add_argument("--scenario-rate", type=float, default=1.0, help="multiplier on scenario injection rates")
    p.add_argument("--faulty-share", type=float, default=C.INITIAL_FAULTY_SHARE,
                   help="fraction of the fleet starting on firmware 3.2")
    p.add_argument("--realtime", action="store_true", help="pace output to wall-clock time")
    args = p.parse_args(argv)

    cfg = SimulationConfig(
        vehicles=args.vehicles, events_per_second=args.events_per_second, duration=args.duration,
        seed=args.seed, start_time=args.start_time, scenario_rate=args.scenario_rate,
        faulty_share=args.faulty_share, realtime=args.realtime,
    )
    started = time.monotonic()
    with JsonlSink(args.output, compress=args.compress) as sink:
        counts = run(cfg, sink)
    elapsed = time.monotonic() - started

    manifest = {
        "config": {**vars(args), "output": str(args.output), "start_time": args.start_time.isoformat()},
        "counts": dict(sorted(counts.items())),
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    events = sum(n for k, n in counts.items() if not k.startswith("scenario:"))
    print(f"wrote {events:,} events to {args.output} in {elapsed:.1f}s ({events / elapsed:,.0f} events/s)",
          file=sys.stderr)
    for k, n in sorted(counts.items()):
        print(f"  {k:32} {n:>10,}", file=sys.stderr)
