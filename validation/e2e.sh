#!/usr/bin/env bash
# End-to-end data-contract test (Phase 4). Wipes local vehicle/validated/quarantine topics.
#
#   simulator (5% bad data) -> vehicle.* (JSON) -> validator -> validated.vehicle.* (Avro) / quarantine.vehicle
#
# Pass = every injected corruption is quarantined with the matching reason, duplicates
# and late events are counted exactly, nothing clean is rejected, a v1 perception
# consumer reads the v2 topic, the registry rejects breaking changes, and the
# Prometheus counters account for every record.
source "$(dirname "$0")/../infrastructure/chaos/lib.sh"

VEHICLES="${VEHICLES:-100}"; DURATION="${DURATION:-60}"; RATE="${RATE:-0.05}"

setup
dc up -d --wait schema-registry >/dev/null
reset_topics

log "registering schemas"
"$PY" -m validation.registry register >/dev/null
evolution=0; "$PY" -m validation.registry evolution | tee "$OUT/evolution.txt" >&2 || evolution=$?

log "producing $VEHICLES vehicles x ${DURATION}s with bad-data rate $RATE"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" --bad-data-rate "$RATE" 2>&1 | grep 'bad data' >&2
produced="$("$PY" -c "import json; print(json.load(open('$OUT/run/manifest.json'))['kafka']['produced'])")"

log "validating"
"$PY" -m validation --idle-timeout 8 --summary-json "$OUT/validator.json" 2>/dev/null & validator=$!
metrics_total=0
for _ in $(seq 1 60); do  # scrape while the validator is still up
  curl -s 127.0.0.1:9108/metrics > "$OUT/metrics.txt" 2>/dev/null || true
  metrics_total="$(awk '/^dq_events_total/ { s += $2 } END { printf "%d", s }' "$OUT/metrics.txt")"
  [[ "$metrics_total" == "$produced" ]] && break
  sleep 1
done
wait "$validator"

log "reading the outputs"
"$PY" -m validation.inspect validated --perception-reader 1 --summary-json "$OUT/validated-v1-reader.json" >/dev/null
"$PY" -m validation.inspect validated --summary-json "$OUT/validated-latest-reader.json" >/dev/null
"$PY" -m validation.inspect quarantine --summary-json "$OUT/quarantine.json" >/dev/null

Q="sum(v for key, v in c.items() if key.startswith('quarantined:'))"
verify --manifest "$OUT/run/manifest.json" --consumer "$OUT/validator.json" \
  --consumer v1="$OUT/validated-v1-reader.json" --consumer latest="$OUT/validated-latest-reader.json" \
  --consumer q="$OUT/quarantine.json" --var evolution="$evolution" --var metrics_total="$metrics_total" \
  "c['consumed'] == k['produced']                                                    :: validator read every raw record" \
  "c.get('quarantined:malformed_json', 0) == m['bad_data']['injected'].get('corrupt_payload', 0)        :: corrupt payloads -> malformed_json" \
  "c.get('quarantined:invalid_timestamp', 0) == m['bad_data']['injected'].get('corrupt_timestamp', 0)   :: corrupt timestamps -> invalid_timestamp" \
  "c.get('quarantined:missing_field', 0) == m['bad_data']['injected'].get('missing_field', 0)           :: missing fields -> missing_field" \
  "c.get('quarantined:invalid_value', 0) == m['bad_data']['injected'].get('invalid_confidence', 0)      :: invalid confidence -> invalid_value" \
  "c.get('quarantined:unknown_enum', 0) == m['bad_data']['injected'].get('unknown_object_type', 0)      :: unknown object types -> unknown_enum" \
  "c.get('quarantined:unsupported_schema_version', 0) == m['bad_data']['injected'].get('old_schema_version', 0) :: old schema versions -> unsupported_schema_version" \
  "c.get('duplicate', 0) == m['bad_data']['injected'].get('duplicate', 0)            :: duplicates dropped exactly" \
  "c.get('late', 0) == m['bad_data']['injected'].get('late', 0)                      :: late events flagged exactly" \
  "c['valid'] == k['produced'] - $Q - c.get('duplicate', 0)                          :: no clean record rejected" \
  "q['quarantined'] == $Q                                                            :: quarantine topic holds every rejected record" \
  "c['version:perception:v1'] > 0 and c['version:perception:v2'] > 0                 :: fleet sends both perception v1 and v2" \
  "v1['decoded'] == c['valid'] and v1['decode_errors'] == 0                          :: v1 perception consumer reads the v2 topic" \
  "'occlusion' not in v1['perception_fields'] and 'occlusion' in latest['perception_fields'] :: v1 reader sees v1 fields, latest sees occlusion" \
  "evolution == 0                                                                    :: registry accepts optional fields, rejects breaking changes" \
  "metrics_total == k['produced']                                                    :: Prometheus dq_events_total accounts for every record"
