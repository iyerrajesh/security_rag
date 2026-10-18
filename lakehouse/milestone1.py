"""Facts for the Milestone 1 exit check: what Trino returns over the curated tables.

    python -m lakehouse.milestone1 --output facts.json
"""

import argparse
import json
import sys
from decimal import Decimal

from lakehouse import trino
from lakehouse.facts import collect
from lakehouse.catalog import get_catalog


def collect_trino() -> dict:
    correlated = trino.run(trino.load("correlated_events"))
    columns = correlated.columns
    complete = sum(all(v is not None for v in row) for row in correlated.rows)
    failures = trino.run(trino.load("pedestrian_detection_failures"))
    versions = trino.run(trino.load("failure_rate_by_version"))
    pedestrians = trino.run(trino.load("pedestrian_confidence_by_firmware"))

    by_firmware = {}
    for row in versions.rows:
        r = dict(zip(versions.columns, row))
        by_firmware.setdefault(r["firmware_version"], {}).update(
            emergency_brakes_per_100=r["emergency_brakes_per_100"], camera_faults_per_100=r["camera_faults_per_100"])
    for row in pedestrians.rows:
        r = dict(zip(pedestrians.columns, row))
        by_firmware.setdefault(r["firmware_version"], {}).update(weak_pct=r["weak_pct"], avg_confidence=r["avg_confidence"])

    counts = {}
    for table in ("vehicle_perception", "vehicle_planning", "vehicle_telemetry", "vehicle_health", "software_deployments"):
        counts[f"curated.{table}"] = trino.run(f"SELECT count(*) FROM curated.{table}").rows[0][0]
    return {"correlated_rows": len(correlated.rows), "correlated_complete_rows": complete, "correlated_columns": columns,
            "failure_rows": len(failures.rows), "by_firmware": by_firmware, "trino_rows": counts}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m lakehouse.milestone1")
    p.add_argument("--output")
    args = p.parse_args(argv)
    facts = collect_trino()
    facts["iceberg_rows"] = {k: v for k, v in collect(get_catalog())["rows"].items() if k in facts["trino_rows"]}
    text = json.dumps(facts, indent=2, sort_keys=True, default=lambda o: float(o) if isinstance(o, Decimal) else str(o))
    if args.output:
        with open(args.output, "w") as f:
            f.write(text)
    print(text, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
