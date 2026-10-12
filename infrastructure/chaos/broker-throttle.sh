#!/usr/bin/env bash
# Throttle producers with a broker-side byte-rate quota. The broker delays its
# responses and the producer slows to match. Pass = rate drops, throttling is
# visible to the client, nothing is lost.
source "$(dirname "$0")/lib.sh"

VEHICLES="${VEHICLES:-300}"; DURATION="${DURATION:-60}"
BYTE_RATE="${BYTE_RATE:-262144}"  # 256 KB/s

remove_quota() {
  kafka_bin kafka-configs.sh --alter --entity-type clients --entity-default \
    --delete-config producer_byte_rate >/dev/null 2>&1 || true
}

setup
trap remove_quota EXIT
remove_quota

reset_topics
log "baseline: $VEHICLES vehicles x ${DURATION}s, no quota"
simulate baseline --vehicles "$VEHICLES" --duration "$DURATION"

reset_topics
log "throttled: producer_byte_rate=$BYTE_RATE"
kafka_bin kafka-configs.sh --alter --entity-type clients --entity-default \
  --add-config producer_byte_rate="$BYTE_RATE" >/dev/null
simulate throttled --vehicles "$VEHICLES" --duration "$DURATION"
remove_quota

log "verifying"
consume "verify-$$" "$OUT/consumer.json" >/dev/null
verify --manifest t="$OUT/throttled/manifest.json" --manifest b="$OUT/baseline/manifest.json" \
  --consumer "$OUT/consumer.json" \
  "t['elapsed_seconds'] > 1.5 * b['elapsed_seconds']                :: throttled run is >1.5x slower than baseline" \
  "k['max_broker_throttle_ms'] > 0                                   :: client saw broker throttle time" \
  "b['kafka']['max_broker_throttle_ms'] == 0                         :: baseline was not throttled" \
  "k['failed'] == 0 and k['delivered'] == k['produced']              :: no delivery failures" \
  "c['consumed'] == k['produced']                                    :: every throttled event consumed"
