"""Read the validator's output topics and summarize them.

    python -m validation.inspect validated                      # decode with the writer (latest) schemas
    python -m validation.inspect validated --perception-reader 1  # decode perception as a v1 consumer would
    python -m validation.inspect quarantine                     # counts by reason, plus samples
"""

import argparse
import json
import sys
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from schemas.avro import load
from validation import registry
from validation.rules import MODELS
from validation.service import QUARANTINE_TOPIC, validated_topic


def consume(bootstrap: str, topics: list[str], idle_timeout: float):
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"inspect-{uuid.uuid4().hex[:8]}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    assigned = False

    def on_assign(*_):
        nonlocal assigned, last
        assigned, last = True, time.monotonic()

    consumer.subscribe(topics, on_assign=on_assign)
    start = last = time.monotonic()
    try:
        while time.monotonic() - last < idle_timeout or (not assigned and time.monotonic() - start < 60):
            msg = consumer.poll(0.5)
            if msg is None or (msg.error() and msg.error().code() == KafkaError._PARTITION_EOF):
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            last = time.monotonic()
            yield msg
    finally:
        consumer.close()


def inspect_validated(args) -> dict:
    sr = registry.client(args.registry_url)
    deserializers = {}
    for event_type in MODELS:
        reader = load("perception", args.perception_reader) if event_type == "perception" and args.perception_reader else None
        deserializers[validated_topic(event_type)] = AvroDeserializer(sr, schema_str=reader)

    counts, fields, errors, late = Counter(), {}, 0, 0
    for msg in consume(args.bootstrap_servers, sorted(deserializers), args.idle_timeout):
        try:
            record = deserializers[msg.topic()](msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        except Exception as exc:
            errors += 1
            if errors <= 3:
                print(f"decode error on {msg.topic()}: {exc}", file=sys.stderr)
            continue
        counts[msg.topic()] += 1
        fields.setdefault(msg.topic(), set()).update(record)
        late += dict(msg.headers() or []).get("late") == b"true"
    return {
        "decoded": sum(counts.values()),
        "decode_errors": errors,
        "late": late,
        "per_topic": dict(sorted(counts.items())),
        "perception_reader": args.perception_reader or "latest",
        "perception_fields": sorted(fields.get(validated_topic("perception"), [])),
    }


def inspect_quarantine(args) -> dict:
    reasons, samples = Counter(), {}
    for msg in consume(args.bootstrap_servers, [QUARANTINE_TOPIC], args.idle_timeout):
        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        reasons[headers["reason"]] += 1
        samples.setdefault(headers["reason"], {"detail": headers["detail"], "source": headers["source_topic"],
                                               "value": msg.value()[:160].decode(errors="replace")})
    return {"quarantined": sum(reasons.values()), "reasons": dict(sorted(reasons.items())), "samples": samples}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m validation.inspect")
    p.add_argument("what", choices=("validated", "quarantine"))
    p.add_argument("--bootstrap-servers", default="127.0.0.1:9094")
    p.add_argument("--registry-url", default=registry.DEFAULT_URL)
    p.add_argument("--perception-reader", type=int, help="decode perception with this schema version's reader schema")
    p.add_argument("--idle-timeout", type=float, default=5.0)
    p.add_argument("--summary-json")
    args = p.parse_args(argv)
    summary = inspect_validated(args) if args.what == "validated" else inspect_quarantine(args)
    if args.summary_json:
        with open(args.summary_json, "w") as f:
            json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
