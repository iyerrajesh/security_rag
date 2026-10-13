"""Validation service: raw vehicle.* (JSON) -> validated.vehicle.* (Avro) or quarantine.vehicle.

    python -m validation                       # run until stopped
    python -m validation --idle-timeout 10     # batch mode: exit after 10 s without input

Delivery is at-least-once. Offsets are committed only after everything produced
for them has been acknowledged, so a crash re-processes records rather than
losing them. Prometheus metrics are served on :9108/metrics.
"""

import argparse
import json
import signal
import sys
import time
from collections import Counter
from datetime import timedelta

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter as PromCounter
from prometheus_client import start_http_server

from schemas import TOPICS
from schemas.avro import to_avro_record
from validation import registry
from validation.rules import MODELS, StreamState, validate

RAW_TOPICS = {topic: model.model_fields["event_type"].default for model, topic in TOPICS.items()}
QUARANTINE_TOPIC = "quarantine.vehicle"
CONTRACT_REFRESH_SECONDS = 30
COMMIT_INTERVAL_SECONDS = 1.0

EVENTS = PromCounter("dq_events", "Raw events processed, by outcome", ["event_type", "outcome"])
QUARANTINED = PromCounter("dq_quarantined", "Events sent to quarantine, by reason", ["event_type", "reason"])
LATE = PromCounter("dq_late_events", "Valid events arriving later than the allowed lateness", ["event_type"])
VERSIONS = PromCounter("dq_schema_version", "Valid events by schema version", ["event_type", "version"])


def validated_topic(event_type: str) -> str:
    return f"validated.vehicle.{event_type}"


class Validator:
    def __init__(self, args):
        self.args = args
        self.sr = registry.client(args.registry_url)
        self.contracts = registry.contracts(self.sr)
        self.contracts_loaded = time.monotonic()
        self.state = StreamState(lateness=timedelta(seconds=args.lateness))
        # Validated topics are written with the latest registered schema of each type;
        # older records (e.g. perception v1) are upcast, with new fields defaulted.
        self.serializers = {
            event_type: AvroSerializer(self.sr, conf={"auto.register.schemas": False, "use.latest.version": True})
            for event_type in MODELS
        }
        self.consumer = Consumer({
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
            "linger.ms": 20,
            "compression.type": "lz4",
        })
        self.totals: Counter = Counter()
        self.delivery_errors = 0
        self.running = True

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.delivery_errors += 1
            print(f"delivery failed: {err}", file=sys.stderr)

    def _produce(self, **kwargs) -> None:
        while True:
            try:
                self.producer.produce(on_delivery=self._on_delivery, **kwargs)
                return
            except BufferError:
                self.producer.poll(0.1)  # backpressure: wait for the broker

    def process(self, msg) -> None:
        event_type = RAW_TOPICS[msg.topic()]
        result = validate(msg.value(), event_type, self.contracts)

        if not result.valid:
            self._produce(
                topic=QUARANTINE_TOPIC,
                key=msg.key(),
                value=msg.value(),  # the original bytes, untouched, for investigation and replay
                headers={
                    "reason": result.reason,
                    "detail": result.detail,
                    "source_topic": msg.topic(),
                    "source_partition": str(msg.partition()),
                    "source_offset": str(msg.offset()),
                },
            )
            EVENTS.labels(event_type, "quarantined").inc()
            QUARANTINED.labels(event_type, result.reason).inc()
            self.totals[f"quarantined:{result.reason}"] += 1
            return

        event = result.event
        if self.state.is_duplicate(event):
            EVENTS.labels(event_type, "duplicate").inc()
            self.totals["duplicate"] += 1
            return

        late = self.state.is_late(event)
        if late:
            LATE.labels(event_type).inc()
            self.totals["late"] += 1
        topic = validated_topic(event_type)
        value = self.serializers[event_type](to_avro_record(event), SerializationContext(topic, MessageField.VALUE))
        self._produce(
            topic=topic,
            key=event.vehicle_id.encode(),
            value=value,
            timestamp=int(event.timestamp.timestamp() * 1000),
            headers={"schema_version": str(event.schema_version), "late": "true" if late else "false"},
        )
        EVENTS.labels(event_type, "valid").inc()
        VERSIONS.labels(event_type, str(event.schema_version)).inc()
        self.totals["valid"] += 1
        self.totals[f"version:{event_type}:v{event.schema_version}"] += 1

    def commit(self) -> None:
        """Flush outputs, then commit inputs: never commit an offset whose output could be lost."""
        if self.producer.flush(30):
            raise RuntimeError("producer flush timed out; not committing")
        if self.delivery_errors:
            raise RuntimeError(f"{self.delivery_errors} deliveries failed; not committing")
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as exc:  # nothing consumed since the last commit
            if "NO_OFFSET" not in str(exc):
                raise

    def run(self) -> dict:
        started = last_commit = time.monotonic()
        self.last_message = started
        assigned = False

        def on_assign(_consumer, _partitions) -> None:
            nonlocal assigned
            assigned = True
            self.last_message = time.monotonic()  # idle clock starts once we own partitions

        self.consumer.subscribe(sorted(RAW_TOPICS), on_assign=on_assign)
        try:
            while self.running:
                now = time.monotonic()
                idle = now - self.last_message
                if self.args.idle_timeout and idle > self.args.idle_timeout and (assigned or idle > 60):
                    break
                if now - self.contracts_loaded > CONTRACT_REFRESH_SECONDS:
                    self.contracts = registry.contracts(self.sr)  # pick up newly registered versions
                    self.contracts_loaded = now
                if now - last_commit > COMMIT_INTERVAL_SECONDS:
                    self.commit()
                    last_commit = now

                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"consumer error: {msg.error()}", file=sys.stderr)
                    continue
                self.last_message = time.monotonic()
                self.totals["consumed"] += 1
                self.process(msg)
            self.commit()
        finally:
            self.consumer.close()

        elapsed = max(self.last_message - started, 1e-9)
        return {"elapsed_seconds": round(elapsed, 2), "rate": round(self.totals["consumed"] / elapsed),
                **dict(sorted(self.totals.items()))}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m validation")
    p.add_argument("--bootstrap-servers", default="127.0.0.1:9094")
    p.add_argument("--registry-url", default=registry.DEFAULT_URL)
    p.add_argument("--group-id", default="validator")
    p.add_argument("--lateness", type=float, default=10.0, help="allowed lateness in seconds (default: 10)")
    p.add_argument("--idle-timeout", type=float, default=0.0, help="exit after this many idle seconds (0 = never)")
    p.add_argument("--metrics-port", type=int, default=9108, help="Prometheus port (0 = disabled)")
    p.add_argument("--summary-json", help="write totals to this file on exit")
    args = p.parse_args(argv)

    if args.metrics_port:
        start_http_server(args.metrics_port, addr="127.0.0.1")
    validator = Validator(args)
    signal.signal(signal.SIGTERM, lambda *_: setattr(validator, "running", False))
    print(f"validating {len(RAW_TOPICS)} topics with {len(validator.contracts)} registered contracts", file=sys.stderr)
    try:
        summary = validator.run()
    except KeyboardInterrupt:
        validator.running = False
        return 130

    if args.summary_json:
        with open(args.summary_json, "w") as f:
            json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
