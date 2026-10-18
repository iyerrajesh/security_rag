#!/usr/bin/env bash
# End-to-end lakehouse test (Phase 5). Wipes local vehicle/validated/quarantine topics AND all lakehouse tables.
#
#   simulator (5% bad data) -> vehicle.* -> validator -> validated.* / quarantine.vehicle -> Iceberg
#                                                      \-> raw sink: vehicle.* -> raw.*
#
# Pass = every record is accounted for in the tables (raw = produced; validated + repeats + quarantine = raw),
# the quarantine table holds each injected corruption with the right reason, the validated and curated tables
# contain zero rows that fail their Pydantic model, and a second run of every sink writes nothing new.
source "$(dirname "$0")/../infrastructure/chaos/lib.sh"

VEHICLES="${VEHICLES:-100}"; DURATION="${DURATION:-60}"; RATE="${RATE:-0.05}"

setup
dc up -d --wait schema-registry minio iceberg-rest >/dev/null
reset_topics

log "registering schemas, recreating lakehouse tables"
"$PY" -m validation.registry register >/dev/null
"$PY" -m lakehouse bootstrap --reset >/dev/null

log "producing $VEHICLES vehicles x ${DURATION}s with bad-data rate $RATE"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" --bad-data-rate "$RATE" 2>&1 | grep 'bad data' >&2
produced="$("$PY" -c "import json; print(json.load(open('$OUT/run/manifest.json'))['kafka']['produced'])")"

log "validating"
"$PY" -m validation --idle-timeout 8 --metrics-port 0 --summary-json "$OUT/validator.json" 2>/dev/null

sinks() {  # sinks <suffix>: run the three sinks in parallel until their topics are idle
  for mode in raw validated quarantine; do
    "$PY" -m lakehouse sink "$mode" --idle-timeout 8 --commit-interval 2 --metrics-port 0 \
      --summary-json "$OUT/sink-$mode$1.json" 2>"$OUT/sink-$mode$1.log" &
  done
  wait
  for mode in raw validated quarantine; do
    [[ -f "$OUT/sink-$mode$1.json" ]] || { tail -5 "$OUT/sink-$mode$1.log" >&2; log "sink $mode failed"; }
  done
}
log "landing to Iceberg"
sinks ""
"$PY" -m lakehouse curate >/dev/null
"$PY" -m lakehouse facts --output "$OUT/facts.json" 2>/dev/null

log "second pass: every sink and the curator must find nothing new"
sinks "-again"
"$PY" -m lakehouse curate >/dev/null
"$PY" -m lakehouse facts --output "$OUT/facts-again.json" 2>/dev/null
"$PY" -m lakehouse status >&2

Q="sum(v for key, v in c.items() if key.startswith('quarantined:'))"
I="m['bad_data']['injected']"
verify --manifest "$OUT/run/manifest.json" --consumer c="$OUT/validator.json" --consumer f="$OUT/facts.json" \
  --consumer g="$OUT/facts-again.json" \
  "f['raw_rows'] == k['produced'] and f['raw_rows'] > 0                                                    :: raw tables hold every produced record" \
  "f['validated_rows'] == c['valid']                                                 :: validated tables hold every valid record" \
  "f['quarantine_rows'] == $Q                                                        :: quarantine table holds every rejected record" \
  "f['validated_rows'] + c.get('duplicate', 0) + f['quarantine_rows'] == f['raw_rows'] :: valid + duplicates + quarantined = raw" \
  "f['quarantine_reasons'].get('malformed_json', 0) == $I.get('corrupt_payload', 0)  :: corrupt payloads quarantined as malformed_json" \
  "f['quarantine_reasons'].get('invalid_timestamp', 0) == $I.get('corrupt_timestamp', 0) :: corrupt timestamps quarantined" \
  "f['quarantine_reasons'].get('missing_field', 0) == $I.get('missing_field', 0)     :: missing fields quarantined" \
  "f['quarantine_reasons'].get('invalid_value', 0) == $I.get('invalid_confidence', 0) :: invalid confidence quarantined" \
  "f['quarantine_reasons'].get('unknown_enum', 0) == $I.get('unknown_object_type', 0) :: unknown object types quarantined" \
  "f['quarantine_reasons'].get('unsupported_schema_version', 0) == $I.get('old_schema_version', 0) :: old schema versions quarantined" \
  "f['corrupt_rows'] == 0 and f['validated_rows'] > 0                                :: zero corrupt rows in validated and curated tables" \
  "f['curated_rows'] == f['curated_distinct'] == f['validated_distinct']             :: curated tables hold each event exactly once" \
  "g['rows'] == f['rows']                                                            :: a second pass writes no new rows (no replay, no repeats)"
