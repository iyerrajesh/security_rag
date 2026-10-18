"""Summarize what is in the lakehouse, for the end-to-end test to check.

    python -m lakehouse facts --output facts.json

Reads the data (not only the metadata): every row of the validated and curated tables is run back
through its Pydantic model, which is the proof that no corrupt row got past the validator.
"""

import argparse
import json
import sys
from collections import Counter

from pydantic import ValidationError
from pyiceberg.catalog import Catalog

from lakehouse.tables import MODELS, all_tables, tables_of


def rows_failing_model(catalog: Catalog, identifier: str, event_type: str) -> int:
    model = MODELS[event_type]
    fields = set(model.model_fields)
    bad = 0
    for row in catalog.load_table(identifier).scan().to_arrow().to_pylist():
        try:
            model.model_validate({k: v for k, v in row.items() if k in fields})
        except ValidationError:
            bad += 1
    return bad


def collect(catalog: Catalog) -> dict:
    facts = {"rows": {}, "corrupt": {}, "distinct_event_ids": {}, "quarantine_reasons": {}}
    for definition in all_tables():
        table = catalog.load_table(definition.identifier)
        facts["rows"][definition.identifier] = table.scan(selected_fields=("kafka_offset",)).to_arrow().num_rows
    for kind in ("validated", "curated"):
        for definition in tables_of(kind):
            facts["corrupt"][definition.identifier] = rows_failing_model(catalog, definition.identifier, definition.event_type)
            ids = catalog.load_table(definition.identifier).scan(selected_fields=("event_id",)).to_arrow()["event_id"]
            facts["distinct_event_ids"][definition.identifier] = len(set(ids.to_pylist()))
    reasons = catalog.load_table("quarantine.vehicle").scan(selected_fields=("reason",)).to_arrow()["reason"]
    facts["quarantine_reasons"] = dict(Counter(reasons.to_pylist()))

    total = lambda kind: sum(v for k, v in facts["rows"].items() if k in {t.identifier for t in tables_of(kind)})  # noqa: E731
    facts["raw_rows"], facts["validated_rows"] = total("raw"), total("validated")
    facts["curated_rows"], facts["quarantine_rows"] = total("curated"), total("quarantine")
    facts["corrupt_rows"] = sum(facts["corrupt"].values())
    facts["curated_distinct"] = sum(v for k, v in facts["distinct_event_ids"].items() if k.startswith("curated."))
    facts["validated_distinct"] = sum(v for k, v in facts["distinct_event_ids"].items() if k.startswith("validated."))
    return facts


def main(argv: list[str], catalog: Catalog) -> int:
    p = argparse.ArgumentParser(prog="python -m lakehouse facts")
    p.add_argument("--output")
    args = p.parse_args(argv)
    facts = collect(catalog)
    text = json.dumps(facts, indent=2, sort_keys=True)
    if args.output:
        with open(args.output, "w") as f:
            f.write(text)
    print(text, file=sys.stderr)
    return 0
