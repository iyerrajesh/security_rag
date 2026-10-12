#!/usr/bin/env bash
# Kill a consumer mid-backlog (SIGKILL, no clean shutdown), restart it in the
# same group, and let it catch up. Pass = it resumes exactly from the last
# committed offsets and drains the backlog. Reports recovery time, which is
# dominated by session.timeout.ms: the dead member keeps its partitions until evicted.
source "$(dirname "$0")/lib.sh"

VEHICLES="${VEHICLES:-1000}"; DURATION="${DURATION:-60}"; KILL_AFTER="${KILL_AFTER:-8}"
GROUP="chaos-restart-$$"

setup
reset_topics

log "building a backlog: $VEHICLES vehicles x ${DURATION}s, no consumer running"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" 2>/dev/null

log "slow consumer, SIGKILL after ${KILL_AFTER}s"
# Start python directly (not via the consume() function) so $! is the consumer
# process itself and SIGKILL hits it, not a wrapper subshell.
"$PY" -m ingestion.consume --from-beginning --group-id "$GROUP" --delay-ms 0.05 >/dev/null 2>&1 & consumer=$!
sleep "$KILL_AFTER"; kill -9 "$consumer"; wait "$consumer" 2>/dev/null || true
committed="$(group_committed "$GROUP")"
log "committed offsets at kill: $committed"

log "restarting consumer in the same group"
# --from-beginning only affects partitions with no committed offset; the rest resume where they were.
# The killed member still owns its partitions until the broker evicts it after
# session.timeout.ms (45 s default), so idle long enough to see that rebalance.
consume "$GROUP" "$OUT/consumer.json" --idle-timeout 60 >/dev/null
final_lag="$(group_lag "$GROUP")"

status=0
verify --manifest "$OUT/run/manifest.json" --consumer "$OUT/consumer.json" \
  --var committed="$committed" --var final_lag="${final_lag:-0}" \
  "0 < committed < k['produced']              :: consumer was killed partway through the backlog" \
  "c['consumed'] == k['produced'] - committed :: restart resumed exactly at the committed offsets" \
  "final_lag == 0                             :: backlog fully drained" || status=1
"$PY" -c "import json,sys; c=json.load(open(sys.argv[1])); print(f\"  info  recovery: {c['consumed']:,} messages in {c['elapsed_seconds']}s, including the wait for the dead member to be evicted\")" \
  "$OUT/consumer.json" | tee -a "$OUT/result.txt"
exit "$status"
