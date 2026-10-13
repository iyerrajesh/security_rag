import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from . import config as C
from .engine import DEFAULT_START, SimulationConfig, run
from .bad_data import BadDataSink
from .sinks import JsonlSink, KafkaSink


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


def _key_value(value: str) -> tuple[str, str]:
    key, sep, val = value.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError("expected key=value")
    return key, val


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="python -m simulator", description="Synthetic AV fleet event generator.")
    p.add_argument("--vehicles", type=_positive(int), default=100)
    p.add_argument("--events-per-second", type=_positive(float), default=1.0,
                   help="report rate per vehicle, in Hz (default: 1)")
    p.add_argument("--duration", type=_positive(float), default=60.0, help="simulated seconds (default: 60)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sink", choices=("jsonl", "kafka"), default="jsonl")
    p.add_argument("--output", type=Path, default=Path("output/sim"),
                   help="output directory (with --sink kafka: ground-truth labels and manifest only)")
    p.add_argument("--compress", action="store_true", help="write .jsonl.gz (jsonl sink)")
    p.add_argument("--bootstrap-servers", default="127.0.0.1:9094", help="Kafka bootstrap servers (kafka sink)")
    p.add_argument("--producer-config", type=_key_value, action="append", default=[], metavar="KEY=VALUE",
                   help="librdkafka producer setting, repeatable, e.g. queue.buffering.max.messages=1000")
    p.add_argument("--start-time", type=_utc,
                   help=f"sim start, ISO 8601 with timezone (default: {DEFAULT_START:%Y-%m-%dT%H:%M:%SZ} for jsonl; "
                        "now for kafka, since Kafka retention deletes messages whose event time is too old)")
    p.add_argument("--scenario-rate", type=float, default=1.0, help="multiplier on scenario injection rates")
    p.add_argument("--faulty-share", type=float, default=C.INITIAL_FAULTY_SHARE,
                   help="fraction of the fleet starting on firmware 3.2")
    p.add_argument("--realtime", action="store_true", help="pace output to wall-clock time")
    p.add_argument("--bad-data-rate", type=float, default=0.0,
                   help="fraction of events to corrupt, 0-1 (see simulator/bad_data.py)")
    args = p.parse_args(argv)
    if args.start_time is None:
        args.start_time = (datetime.now(timezone.utc).replace(microsecond=0) if args.sink == "kafka"
                           else DEFAULT_START)

    cfg = SimulationConfig(
        vehicles=args.vehicles, events_per_second=args.events_per_second, duration=args.duration,
        seed=args.seed, start_time=args.start_time, scenario_rate=args.scenario_rate,
        faulty_share=args.faulty_share, realtime=args.realtime,
    )
    started = time.monotonic()
    if args.sink == "kafka":
        sink = KafkaSink(args.bootstrap_servers, args.output, **dict(args.producer_config))
    else:
        sink = JsonlSink(args.output, compress=args.compress)
    outer = BadDataSink(sink, args.bad_data_rate, args.seed) if args.bad_data_rate > 0 else sink
    error = None
    try:
        counts = run(cfg, outer)
    finally:
        try:
            outer.close()
        except RuntimeError as exc:  # delivery failures: still write the manifest, then fail
            error = exc
    elapsed = time.monotonic() - started

    manifest = {
        "config": {**vars(args), "output": str(args.output), "start_time": args.start_time.isoformat(),
                   "producer_config": dict(args.producer_config)},
        "counts": dict(sorted(counts.items())),
        "elapsed_seconds": round(elapsed, 2),
    }
    if isinstance(sink, KafkaSink):
        manifest["kafka"] = sink.stats()
    if isinstance(outer, BadDataSink):
        manifest["bad_data"] = {"rate": args.bad_data_rate, "injected": dict(sorted(outer.injected.items()))}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    events = sum(n for k, n in counts.items() if not k.startswith("scenario:"))
    print(f"wrote {events:,} events to {args.output} in {elapsed:.1f}s ({events / elapsed:,.0f} events/s)",
          file=sys.stderr)
    for k, n in sorted(counts.items()):
        print(f"  {k:32} {n:>10,}", file=sys.stderr)
    if isinstance(sink, KafkaSink):
        k = manifest["kafka"]
        print(f"  kafka: delivered={k['delivered']:,} failed={k['failed']:,} queue_full={k['queue_full']:,} "
              f"blocked={k['blocked_seconds']}s max_queue={k['max_queue_depth']:,} "
              f"max_throttle={k['max_broker_throttle_ms']}ms", file=sys.stderr)
    if isinstance(outer, BadDataSink):
        print(f"  bad data injected: {dict(sorted(outer.injected.items()))}", file=sys.stderr)
    if error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
