"""Consume the vehicle topics and verify what arrived.

Checks that every message parses against `schemas/`, that the message key
matches `vehicle_id`, and that each vehicle's events stay in timestamp order
(the per-key ordering Kafka promises when producers key by vehicle_id).
Exits after `--idle-timeout` seconds with no new messages.

    python -m ingestion.consume --from-beginning
"""

import argparse
import json
import sys
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

from schemas import TOPICS, parse_event


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m ingestion.consume")
    p.add_argument("--bootstrap-servers", default="127.0.0.1:9094")
    p.add_argument("--group-id", help="consumer group (default: a fresh group, so nothing is committed for others)")
    p.add_argument("--from-beginning", action="store_true", help="start at the earliest offset for a new group")
    p.add_argument("--idle-timeout", type=float, default=5.0)
    p.add_argument("--delay-ms", type=float, default=0.0, help="sleep per message, to simulate a slow consumer")
    p.add_argument("--summary-json", help="also write the results to this file")
    args = p.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id or f"verify-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    assigned = False

    def on_assign(_consumer, partitions) -> None:
        nonlocal assigned, last_message
        assigned = True
        last_message = time.monotonic()  # idle clock starts once we own partitions

    consumer.subscribe(sorted(TOPICS.values()), on_assign=on_assign)

    counts, invalid, key_mismatch, out_of_order = Counter(), Counter(), 0, 0
    last_ts: dict[tuple[str, str], object] = {}
    started = last_message = time.monotonic()
    try:
        # Joining a group can take seconds (longer on a fresh broker), so wait up to 60 s for it.
        while (time.monotonic() - last_message < args.idle_timeout
               or (not assigned and time.monotonic() - started < 60)):
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"error: {msg.error()}", file=sys.stderr)
                continue
            if args.delay_ms:
                time.sleep(args.delay_ms / 1000)
            last_message = time.monotonic()
            counts[msg.topic()] += 1
            try:
                event = parse_event(msg.value())
            except ValidationError:
                invalid[msg.topic()] += 1
                continue
            if msg.key() != event.vehicle_id.encode():
                key_mismatch += 1
            stream = (msg.topic(), event.vehicle_id)
            if stream in last_ts and event.timestamp < last_ts[stream]:
                out_of_order += 1
            last_ts[stream] = event.timestamp
    finally:
        consumer.close()

    total = sum(counts.values())
    elapsed = max(last_message - started, 1e-9)
    print(f"consumed {total:,} messages in {elapsed:.1f}s ({total / elapsed:,.0f} msg/s)")
    for topic in sorted(TOPICS.values()):
        print(f"  {topic:22} {counts[topic]:>10,}  invalid={invalid[topic]}")
    print(f"vehicles={len({v for _, v in last_ts})} key_mismatch={key_mismatch} out_of_order={out_of_order}")
    if args.summary_json:
        with open(args.summary_json, "w") as f:
            json.dump({
                "consumed": total,
                "elapsed_seconds": round(elapsed, 2),
                "rate": round(total / elapsed),
                "per_topic": {t: counts[t] for t in sorted(TOPICS.values())},
                "invalid": sum(invalid.values()),
                "key_mismatch": key_mismatch,
                "out_of_order": out_of_order,
                "vehicles": len({v for _, v in last_ts}),
            }, f, indent=2)
    return 1 if (sum(invalid.values()) or key_mismatch or out_of_order) else 0


if __name__ == "__main__":
    sys.exit(main())
