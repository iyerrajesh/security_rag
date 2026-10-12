#!/usr/bin/env bash
# Pause the broker mid-run. The producer queue fills, the simulator blocks, and
# after unpause everything drains. Pass = nothing lost and the block was recorded.
source "$(dirname "$0")/lib.sh"

VEHICLES="${VEHICLES:-1000}"; DURATION="${DURATION:-60}"
PAUSE_AFTER="${PAUSE_AFTER:-3}"; PAUSE_SECONDS="${PAUSE_SECONDS:-20}"

setup
trap 'dc unpause kafka >/dev/null 2>&1 || true' EXIT
reset_topics

log "producing $VEHICLES vehicles x ${DURATION}s; pausing broker for ${PAUSE_SECONDS}s after ${PAUSE_AFTER}s"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" & sim=$!
sleep "$PAUSE_AFTER"; dc pause kafka >/dev/null
sleep "$PAUSE_SECONDS"; dc unpause kafka >/dev/null
wait "$sim"

log "verifying"
consume "verify-$$" "$OUT/consumer.json" >/dev/null
verify --manifest "$OUT/run/manifest.json" --consumer "$OUT/consumer.json" --var pause="$PAUSE_SECONDS" \
  "k['queue_full'] > 0                         :: producer hit a full queue (backpressure engaged)" \
  "k['blocked_seconds'] >= pause * 0.5          :: simulator blocked for most of the pause" \
  "k['failed'] == 0                             :: no delivery failures" \
  "k['delivered'] == k['produced']              :: every produced event acknowledged" \
  "c['consumed'] == k['produced']               :: every produced event consumed (no loss, no duplicates)" \
  "c['out_of_order'] == 0 and c['invalid'] == 0 :: per-vehicle order preserved, all valid"
