#!/usr/bin/env bash
# Create the vehicle topics. Idempotent: safe to re-run.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-604800000}"  # 7 days: enough to replay a week of data
TOPICS_BIN=/opt/kafka/bin/kafka-topics.sh

for topic in vehicle.telemetry vehicle.perception vehicle.planning vehicle.health vehicle.software; do
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1 \
    --config retention.ms="$RETENTION_MS" \
    --config cleanup.policy=delete \
    --config compression.type=producer
done

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic 'vehicle\..*' | grep -v '^\s*Topic: .*Partition:'
