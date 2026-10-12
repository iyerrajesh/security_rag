#!/usr/bin/env bash
# Cap the producer's local queue at QUEUE_MAX messages, then stall the broker
# briefly. Pass = memory stays bounded (queue never exceeds the cap), the
# simulator blocks instead of buffering, nothing is lost.
source "$(dirname "$0")/lib.sh"

VEHICLES="${VEHICLES:-1000}"; DURATION="${DURATION:-30}"
QUEUE_MAX="${QUEUE_MAX:-1000}"; PAUSE_AFTER="${PAUSE_AFTER:-2}"; PAUSE_SECONDS="${PAUSE_SECONDS:-5}"

setup
trap 'dc unpause kafka >/dev/null 2>&1 || true' EXIT
reset_topics

log "queue.buffering.max.messages=$QUEUE_MAX; pausing broker ${PAUSE_SECONDS}s"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" \
  --producer-config queue.buffering.max.messages="$QUEUE_MAX" & sim=$!
sleep "$PAUSE_AFTER"; dc pause kafka >/dev/null
sleep "$PAUSE_SECONDS"; dc unpause kafka >/dev/null
wait "$sim"

log "verifying"
consume "verify-$$" "$OUT/consumer.json" >/dev/null
# len(producer) counts in-flight protocol requests as well as queued messages,
# so it can read a few above the message cap; allow a small margin.
verify --manifest "$OUT/run/manifest.json" --consumer "$OUT/consumer.json" --var cap="$QUEUE_MAX" \
  "k['max_queue_depth'] <= cap + 20                  :: queue stayed at the cap (bounded memory)" \
  "k['queue_full'] > 0                               :: producer hit the cap and blocked" \
  "k['failed'] == 0 and k['delivered'] == k['produced'] :: no delivery failures" \
  "c['consumed'] == k['produced']                    :: every produced event consumed"
