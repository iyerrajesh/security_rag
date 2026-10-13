"""Register the event schemas with the schema registry and read them back as contracts.

    python -m validation.registry register   # idempotent; registers every version in order
    python -m validation.registry list
    python -m validation.registry evolution  # show what the registry accepts and rejects

Subjects follow the TopicNameStrategy: `validated.vehicle.<type>-value`.
Compatibility is FULL: a new version must be readable by old readers and must
read old data. In practice, only optional fields with defaults may be added.
"""

import argparse
import json
import sys

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from schemas.avro import all_schemas, load
from validation.rules import MODELS, Contract

DEFAULT_URL = "http://127.0.0.1:8081"
COMPATIBILITY = "FULL"


def subject(event_type: str) -> str:
    return f"validated.vehicle.{event_type}-value"


def client(url: str = DEFAULT_URL) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


def register(sr: SchemaRegistryClient) -> None:
    for event_type, model in MODELS.items():
        name = subject(event_type)
        sr.set_compatibility(subject_name=name, level=COMPATIBILITY)
        for version in range(1, model.latest_version() + 1):
            schema_id = sr.register_schema(name, Schema(load(event_type, version), "AVRO"))
            print(f"  {name:40} v{version} -> schema id {schema_id}")


def contracts(sr: SchemaRegistryClient) -> dict[tuple[str, int], Contract]:
    """Every registered version of every event type, keyed by (event_type, schema_version)."""
    out = {}
    for event_type in MODELS:
        for registry_version in sr.get_versions(subject(event_type)):
            registered = sr.get_version(subject(event_type), registry_version)
            contract = Contract.from_avro(event_type, json.loads(registered.schema.schema_str))
            out[(event_type, contract.version)] = contract
    return out


def evolution(sr: SchemaRegistryClient) -> bool:
    """Ask the registry about three candidate perception changes. Returns True if it judged them correctly."""
    v2 = json.loads(load("perception", 2))

    def with_field(field: dict) -> Schema:
        return Schema(json.dumps({**v2, "fields": v2["fields"] + [field]}), "AVRO")

    cases = [
        ("v2 (adds optional occlusion)", Schema(load("perception", 2), "AVRO"), True),
        ("add optional field with default", with_field({"name": "track_age_ms", "type": ["null", "long"], "default": None}), True),
        ("add REQUIRED field (no default)", with_field({"name": "lane_id", "type": "string"}), False),
        ("remove required field `confidence`",
         Schema(json.dumps({**v2, "fields": [f for f in v2["fields"] if f["name"] != "confidence"]}), "AVRO"), False),
    ]
    ok = True
    for label, schema, expected in cases:
        compatible = sr.test_compatibility(subject("perception"), schema, version="latest")
        ok &= compatible == expected
        print(f"  {'compatible  ' if compatible else 'INCOMPATIBLE'}  {label}"
              f"{'' if compatible == expected else '   <-- unexpected'}")
    return ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m validation.registry")
    p.add_argument("command", choices=("register", "list", "evolution"))
    p.add_argument("--url", default=DEFAULT_URL)
    args = p.parse_args(argv)
    sr = client(args.url)

    if args.command == "register":
        register(sr)
    elif args.command == "list":
        for (event_type, version), c in sorted(contracts(sr).items()):
            print(f"  {subject(event_type):40} v{version}  {len(c.fields)} fields ({len(c.required)} required)")
    elif args.command == "evolution":
        return 0 if evolution(sr) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
