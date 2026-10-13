#!/usr/bin/env bash
# Create the vehicle topics. Idempotent: safe to re-run.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-604800000}"  # 7 days: enough to replay a week of data
TOPICS_BIN=/opt/kafka/bin/kafka-topics.sh

QUARANTINE_RETENTION_MS="${QUARANTINE_RETENTION_MS:-2592000000}"  # 30 days: time to investigate bad data

create() {  # create <topic> <retention.ms>
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$PARTITIONS" --replication-factor 1 \
    --config retention.ms="$2" \
    --config cleanup.policy=delete \
    --config compression.type=producer
}

for type in telemetry perception planning health software; do
  create "vehicle.$type" "$RETENTION_MS"            # raw JSON from the fleet (untrusted)
  create "validated.vehicle.$type" "$RETENTION_MS"  # Avro, passed validation (Phase 4)
done
create quarantine.vehicle "$QUARANTINE_RETENTION_MS"  # rejected records + reason headers

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic '(vehicle|validated|quarantine)\..*' \
  | grep -v '^\s*Topic: .*Partition:'
