"""Event sinks. JSONL first; a Kafka sink will implement the same interface."""

import gzip
import io
import json
from pathlib import Path
from typing import Protocol, TextIO

from schemas import BaseEvent


class Sink(Protocol):
    def write(self, event: BaseEvent) -> None: ...
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
        self._file(event.event_type).write(event.model_dump_json() + "\n")

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
        self.labels: list[dict] = []

    def write(self, event: BaseEvent) -> None:
        self.events.append(event)

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
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,  # acks=all, no duplicates on producer retry
            "partitioner": "murmur2_random",  # same key -> partition mapping as Java/Flink producers
            "linger.ms": 20,
            "batch.size": 256 * 1024,
            "compression.type": "lz4",
            **producer_config,
        })
        self._labels = JsonlSink(label_dir)
        self.delivered = 0
        self.failed = 0
        self.first_error = None

    def _on_delivery(self, err, _msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            self.first_error = self.first_error or err

    def write(self, event: BaseEvent) -> None:
        kwargs = dict(
            topic=self._topics[event.event_type],
            key=event.vehicle_id.encode(),
            value=event.model_dump_json().encode(),
            timestamp=int(event.timestamp.timestamp() * 1000),  # Kafka timestamp = event time
            headers={"schema_version": str(event.schema_version)},
            on_delivery=self._on_delivery,
        )
        while True:
            try:
                self._producer.produce(**kwargs)
                break
            except BufferError:
                # Local queue full: backpressure. Serve delivery callbacks until there is room.
                self._producer.poll(0.1)
        self._producer.poll(0)

    def write_label(self, record: dict) -> None:
        self._labels.write_label(record)

    def close(self) -> None:
        remaining = self._producer.flush(60)
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
