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
