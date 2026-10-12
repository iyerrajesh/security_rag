#!/usr/bin/env bash
# Pause the broker for longer than message.timeout.ms. Messages expire in the
# producer queue. Pass = the failure is loud (non-zero exit, failures counted),
# never silent loss.
source "$(dirname "$0")/lib.sh"

VEHICLES="${VEHICLES:-1000}"; DURATION="${DURATION:-60}"
TIMEOUT_MS="${TIMEOUT_MS:-5000}"; PAUSE_AFTER="${PAUSE_AFTER:-3}"; PAUSE_SECONDS="${PAUSE_SECONDS:-15}"

setup
trap 'dc unpause kafka >/dev/null 2>&1 || true' EXIT
reset_topics

log "message.timeout.ms=$TIMEOUT_MS; pausing broker for ${PAUSE_SECONDS}s"
set +e
simulate run --vehicles "$VEHICLES" --duration "$DURATION" \
  --producer-config message.timeout.ms="$TIMEOUT_MS" & sim=$!
sleep "$PAUSE_AFTER"; dc pause kafka >/dev/null
sleep "$PAUSE_SECONDS"; dc unpause kafka >/dev/null
wait "$sim"; exit_code=$?
set -e

log "verifying (simulator exit code: $exit_code)"
consume "verify-$$" "$OUT/consumer.json" >/dev/null
# A timed-out message may still have reached the broker (its request was in flight),
# so consumed can exceed delivered. It can never exceed produced.
verify --manifest "$OUT/run/manifest.json" --consumer "$OUT/consumer.json" --var exit_code="$exit_code" \
  "exit_code != 0                                    :: simulator exited non-zero" \
  "k['failed'] > 0                                   :: delivery failures counted" \
  "'timed out' in (k['first_error'] or '').lower()  :: failure reason is a message timeout" \
  "k['delivered'] + k['failed'] == k['produced']     :: every event accounted for (delivered or failed)" \
  "k['delivered'] <= c['consumed'] <= k['produced']  :: consumed is between delivered and produced"
