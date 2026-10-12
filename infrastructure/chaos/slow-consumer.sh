#!/usr/bin/env bash
# A consumer slower than the producer. Kafka does not slow the producer down;
# the consumer falls behind (lag), then catches up once production stops.
# Pass = lag builds while producing, returns to 0, nothing lost.
source "$(dirname "$0")/lib.sh"

VEHICLES="${VEHICLES:-100}"; DURATION="${DURATION:-30}"; DELAY_MS="${DELAY_MS:-2}"
GROUP="chaos-slow-$$"

setup
reset_topics

log "real-time producer ($VEHICLES vehicles x ${DURATION}s) vs consumer sleeping ${DELAY_MS}ms/message"
consume "$GROUP" "$OUT/consumer.json" --delay-ms "$DELAY_MS" --idle-timeout 20 >/dev/null & consumer=$!
simulate run --vehicles "$VEHICLES" --duration "$DURATION" --realtime 2>/dev/null & sim=$!

echo "seconds,lag" > "$OUT/lag.csv"
start=$SECONDS
while kill -0 "$consumer" 2>/dev/null; do
  lag="$(group_lag "$GROUP")"
  [[ -n "$lag" ]] && echo "$((SECONDS - start)),$lag" | tee -a "$OUT/lag.csv" >&2
  sleep 2
done
wait "$sim"; wait "$consumer" || true
final_lag="$(group_lag "$GROUP")"

log "verifying (lag samples in $OUT/lag.csv)"
peak_lag="$(awk -F, 'NR>1 && $2>m {m=$2} END {print m+0}' "$OUT/lag.csv")"
verify --manifest "$OUT/run/manifest.json" --consumer "$OUT/consumer.json" \
  --var peak_lag="$peak_lag" --var final_lag="${final_lag:-0}" \
  "peak_lag > 1000                         :: lag built up while producing (peak $peak_lag)" \
  "final_lag == 0                          :: consumer caught up (final committed lag 0)" \
  "k['queue_full'] == 0                    :: producer was never blocked by the slow consumer" \
  "c['consumed'] == k['produced']          :: every produced event consumed"
