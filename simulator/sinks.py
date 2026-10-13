"""Event sinks. JSONL first; a Kafka sink will implement the same interface."""

import gzip
import io
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Protocol, TextIO

from schemas import BaseEvent


class Sink(Protocol):
    def write(self, event: BaseEvent) -> None: ...
    def write_raw(self, event_type: str, key: str, payload: str, timestamp: datetime) -> None:
        """Write a payload as-is, without validation (used to inject bad data)."""
    def write_label(self, record: dict) -> None: ...
    def close(self) -> None: ...


class JsonlSink:
    """One file per event type (mirroring the Kafka topics) plus `scenarios.jsonl` labels."""

    def __init__(self, out_dir: Path, compress: bool = False):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.compress = compress
        self._files: dict[str, TextIO] = {}

    def _file(self, name: str) -> TextIO:
        if name not in self._files:
            path = self.out_dir / f"{name}.jsonl"
            if self.compress:
                # mtime=0 keeps output byte-identical across runs with the same seed.
                raw = gzip.GzipFile(f"{path}.gz", mode="wb", mtime=0)
                self._files[name] = io.TextIOWrapper(raw, encoding="utf-8")
            else:
                self._files[name] = open(path, "w", encoding="utf-8")
        return self._files[name]

    def write(self, event: BaseEvent) -> None:
        self._file(event.event_type).write(event.to_wire_json() + "\n")

    def write_raw(self, event_type: str, key: str, payload: str, timestamp: datetime) -> None:
        self._file(event_type).write(payload + "\n")

    def write_label(self, record: dict) -> None:
        self._file("scenarios").write(json.dumps(record) + "\n")

    def close(self) -> None:
        for f in self._files.values():
            f.close()
        self._files.clear()

    def __enter__(self) -> "JsonlSink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class MemorySink:
    """Collects events in memory; used by tests."""

    def __init__(self):
        self.events: list[BaseEvent] = []
        self.raw: list[tuple[str, str]] = []  # (event_type, payload) written with write_raw
        self.labels: list[dict] = []

    def write(self, event: BaseEvent) -> None:
        self.events.append(event)

    def write_raw(self, event_type: str, key: str, payload: str, timestamp: datetime) -> None:
        self.raw.append((event_type, payload))

    def write_label(self, record: dict) -> None:
        self.labels.append(record)

    def close(self) -> None:
        pass


class KafkaSink:
    """Produces events to `vehicle.<event_type>` topics, keyed by `vehicle_id`.

    Keying by vehicle keeps each vehicle's events ordered within one partition.
    Ground-truth labels are not events, so they go to `<label_dir>/scenarios.jsonl`.
    """

    def __init__(self, bootstrap_servers: str, label_dir: Path, **producer_config):
        from confluent_kafka import Producer  # optional dependency: only needed for this sink

        from schemas import TOPICS

        self._topics = {model.model_fields["event_type"].default: topic for model, topic in TOPICS.items()}
        config = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,  # acks=all, no duplicates on producer retry
            "partitioner": "murmur2_random",  # same key -> partition mapping as Java/Flink producers
            "linger.ms": 20,
            "batch.size": 256 * 1024,
            "compression.type": "lz4",
            "statistics.interval.ms": 1000,
            **producer_config,
        }
        self._producer = Producer({**config, "stats_cb": self._on_stats})
        self.producer_config = config
        self._labels = JsonlSink(label_dir)

        # Delivery and backpressure metrics, reported by stats()
        self.produced = 0
        self.delivered = 0
        self.failed = 0
        self.first_error = None
        self.queue_full = 0  # times produce() hit a full local queue
        self.blocked_seconds = 0.0  # time spent waiting for queue space
        self.max_queue_depth = 0
        self.max_broker_rtt_ms = 0.0
        self.max_broker_throttle_ms = 0.0
        self.timeline: list[dict] = []  # one sample per wall-clock second
        self._started = time.monotonic()
        self._next_sample = self._started

    def _on_delivery(self, err, _msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            self.first_error = self.first_error or str(err)

    def _on_stats(self, stats_json: str) -> None:
        stats = json.loads(stats_json)
        for broker in stats.get("brokers", {}).values():
            if broker.get("nodeid", -1) < 0:
                continue  # bootstrap placeholder, not a real broker
            self.max_broker_rtt_ms = max(self.max_broker_rtt_ms, broker["rtt"]["max"] / 1000)
            self.max_broker_throttle_ms = max(self.max_broker_throttle_ms, broker["throttle"]["max"])

    def _sample(self, now: float) -> None:
        depth = len(self._producer)
        self.max_queue_depth = max(self.max_queue_depth, depth)
        if now >= self._next_sample:
            self._next_sample = now + 1.0
            self.timeline.append({
                "t": round(now - self._started, 1),
                "produced": self.produced,
                "delivered": self.delivered,
                "failed": self.failed,
                "queue_depth": depth,
                "blocked_seconds": round(self.blocked_seconds, 2),
            })

    def write(self, event: BaseEvent) -> None:
        self.write_raw(event.event_type, event.vehicle_id, event.to_wire_json(), event.timestamp)

    def write_raw(self, event_type: str, key: str, payload: str, timestamp: datetime) -> None:
        kwargs = dict(
            topic=self._topics[event_type],
            key=key.encode(),
            value=payload.encode(),
            timestamp=int(timestamp.timestamp() * 1000),  # Kafka timestamp = event time
            on_delivery=self._on_delivery,
        )
        try:
            self._producer.produce(**kwargs)
        except BufferError:
            # Local queue full: backpressure. Block the simulator, serving delivery
            # callbacks, until there is room. Nothing is dropped.
            self.queue_full += 1
            blocked_from = time.monotonic()
            while True:
                self._producer.poll(0.1)
                self._sample(time.monotonic())
                try:
                    self._producer.produce(**kwargs)
                    break
                except BufferError:
                    pass
            self.blocked_seconds += time.monotonic() - blocked_from
        self.produced += 1
        self._producer.poll(0)
        self._sample(time.monotonic())

    def write_label(self, record: dict) -> None:
        self._labels.write_label(record)

    def stats(self) -> dict:
        return {
            "produced": self.produced,
            "delivered": self.delivered,
            "failed": self.failed,
            "first_error": self.first_error,
            "queue_full": self.queue_full,
            "blocked_seconds": round(self.blocked_seconds, 2),
            "max_queue_depth": self.max_queue_depth,
            "max_broker_rtt_ms": round(self.max_broker_rtt_ms, 1),
            "max_broker_throttle_ms": self.max_broker_throttle_ms,
            "producer_config": {k: v for k, v in self.producer_config.items() if k != "bootstrap.servers"},
            "timeline": self.timeline,
        }

    def close(self) -> None:
        # Flush in steps so the timeline keeps recording while the queue drains.
        deadline = time.monotonic() + float(self.producer_config.get("message.timeout.ms", 300_000)) / 1000 + 10
        while len(self._producer) and time.monotonic() < deadline:
            self._producer.flush(1.0)
            self._sample(time.monotonic())
        self._next_sample = 0.0
        self._sample(time.monotonic())  # final sample
        remaining = len(self._producer)
        self._labels.close()
        if remaining or self.failed:
            raise RuntimeError(
                f"Kafka delivery incomplete: {self.failed} failed, {remaining} undelivered "
                f"(first error: {self.first_error})"
            )

    def __enter__(self) -> "KafkaSink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
