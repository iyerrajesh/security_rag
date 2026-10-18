#!/usr/bin/env bash
# The small-file experiment (Phase 5). Wipes local vehicle/validated/quarantine topics AND all lakehouse tables.
#
# Commits small batches on purpose (the sinks flush every --batch-rows rows), so each commit adds a few KB of
# Parquet per partition. Then compacts and times the same queries before and after. Results: $OUT/compaction.json
source "$(dirname "$0")/../infrastructure/chaos/lib.sh"

VEHICLES="${VEHICLES:-200}"; DURATION="${DURATION:-120}"; BATCH_ROWS="${BATCH_ROWS:-1000}"
TABLES="${TABLES:-raw.raw_perception validated.validated_perception}"

setup
dc up -d --wait schema-registry minio iceberg-rest >/dev/null
reset_topics
"$PY" -m validation.registry register >/dev/null
"$PY" -m lakehouse bootstrap --reset >/dev/null

log "producing $VEHICLES vehicles x ${DURATION}s"
simulate run --vehicles "$VEHICLES" --duration "$DURATION" --bad-data-rate 0.05 2>&1 | grep 'delivered' >&2
"$PY" -m validation --idle-timeout 8 --metrics-port 0 >/dev/null 2>&1

log "landing with $BATCH_ROWS-row commits (the small-file problem)"
for mode in raw validated quarantine; do
  "$PY" -m lakehouse sink "$mode" --idle-timeout 8 --commit-interval 2 --batch-rows "$BATCH_ROWS" --metrics-port 0 \
    >/dev/null 2>"$OUT/sink-$mode.log" &
done
wait
"$PY" -m lakehouse facts --output "$OUT/facts-before.json" 2>/dev/null
echo "before:" >&2; "$PY" -m lakehouse status >&2

log "compacting and benchmarking: $TABLES"
"$PY" -m lakehouse compact $TABLES --benchmark --output "$OUT/compaction.json" >&2
"$PY" -m lakehouse facts --output "$OUT/facts-after.json" 2>/dev/null

log "expiring snapshots and removing orphan files (older than 0s: nothing is writing)"
"$PY" -m lakehouse expire $TABLES --older-than 0s >&2
"$PY" -m lakehouse orphans $TABLES --older-than 0s >&2
echo "after:" >&2; "$PY" -m lakehouse status >&2
"$PY" -m lakehouse facts --output "$OUT/facts-final.json" 2>/dev/null

verify --consumer b="$OUT/facts-before.json" --consumer a="$OUT/facts-after.json" --consumer z="$OUT/facts-final.json" \
  --consumer comp="$OUT/compaction.json" \
  "b['raw_rows'] > 0 and a['rows'] == b['rows']                                :: compaction changed no table's row count" \
  "z['rows'] == b['rows']                                                       :: expiry and orphan removal changed no row count" \
  "all(r['compaction']['files_after'] * 5 <= r['compaction']['files_before'] for r in comp) :: compaction cut the file count at least 5x" \
  "all(r['after']['plan_ms'] < r['before']['plan_ms'] for r in comp)            :: planning got faster"
