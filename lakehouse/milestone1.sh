#!/usr/bin/env bash
# Milestone 1 exit check: one command runs the simulator for 10 minutes, and a Trino query returns correlated
# perception, planning and telemetry rows. Wipes local vehicle/validated/quarantine topics AND all lakehouse tables.
#
#   simulator -> Kafka -> validator -> Iceberg (raw, validated, curated, quarantine) -> Trino
source "$(dirname "$0")/../infrastructure/chaos/lib.sh"

VEHICLES="${VEHICLES:-100}"; DURATION="${DURATION:-600}"; RATE="${RATE:-0.05}"

setup
dc up -d --wait schema-registry minio iceberg-rest trino >/dev/null
reset_topics
"$PY" -m validation.registry register >/dev/null
"$PY" -m lakehouse bootstrap --reset >/dev/null

log "simulating $VEHICLES vehicles x ${DURATION}s (bad-data rate $RATE)"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" --bad-data-rate "$RATE" 2>&1 | grep 'delivered' >&2
produced="$("$PY" -c "import json; print(json.load(open('$OUT/run/manifest.json'))['kafka']['produced'])")"

log "validating and landing in Iceberg"
"$PY" -m validation --idle-timeout 8 --metrics-port 0 --summary-json "$OUT/validator.json" >/dev/null 2>&1
for mode in raw validated quarantine; do
  "$PY" -m lakehouse sink "$mode" --idle-timeout 8 --metrics-port 0 >/dev/null 2>"$OUT/sink-$mode.log" &
done
wait
"$PY" -m lakehouse curate >&2

log "querying with Trino"
"$PY" -m lakehouse query correlated_events >&2
"$PY" -m lakehouse query failure_rate_by_version >&2
"$PY" -m lakehouse query pedestrian_confidence_by_firmware >&2
"$PY" -m lakehouse.milestone1 --output "$OUT/facts.json" 2>/dev/null

verify --manifest "$OUT/run/manifest.json" --consumer f="$OUT/facts.json" \
  "f['correlated_rows'] > 0 and f['correlated_complete_rows'] == f['correlated_rows'] :: Trino returns correlated perception, planning and telemetry rows" \
  "f['failure_rows'] > 0                                                              :: the pedestrian-detection-failure join finds failures" \
  "f['trino_rows'] == f['iceberg_rows']                                               :: Trino and PyIceberg count the same rows in every curated table" \
  "f['by_firmware']['3.2']['weak_pct'] > 2 * f['by_firmware']['3.1']['weak_pct']       :: firmware 3.2 pedestrians are far more often below the trust threshold" \
  "f['by_firmware']['3.2']['camera_faults_per_100'] > 3 * f['by_firmware']['3.1']['camera_faults_per_100'] :: firmware 3.2 has far more camera faults" \
  "f['by_firmware']['3.2']['emergency_brakes_per_100'] > 3 * f['by_firmware']['3.1']['emergency_brakes_per_100'] :: firmware 3.2 brakes in an emergency far more often"
