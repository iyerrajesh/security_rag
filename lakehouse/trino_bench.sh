#!/usr/bin/env bash
# Trino benchmarks (Phase 6): partition pruning, Parquet column pruning, temporal joins, pre-aggregation, latency.
#
# Needs more than one day of data, which Kafka cannot hold (retention counts from event time), so this simulates
# DAYS separate days straight to files and loads them into the `bench` namespace. The live tables are untouched.
# Results: output/chaos/trino_bench/results.json (and the table printed at the end).
source "$(dirname "$0")/../infrastructure/chaos/lib.sh"

DAYS="${DAYS:-7}"; VEHICLES="${VEHICLES:-100}"; DURATION="${DURATION:-600}"
BASE="${BASE:-2026-10-01}"

setup
dc up -d --wait minio iceberg-rest trino >/dev/null

log "simulating $DAYS days x $VEHICLES vehicles x ${DURATION}s"
mkdir -p "$OUT/days"
for d in $(seq 0 $((DAYS - 1))); do
  day="$(date -u -j -v+${d}d -f %Y-%m-%d "$BASE" +%Y-%m-%d 2>/dev/null || date -u -d "$BASE +$d day" +%Y-%m-%d)"
  "$PY" -m simulator --vehicles "$VEHICLES" --duration "$DURATION" --seed "$d" --start-time "${day}T00:00:00Z" \
    --output "$OUT/days/day$d" >/dev/null 2>&1
done

log "loading into the bench namespace"
"$PY" -m lakehouse backfill "$OUT"/days/day* --namespace bench --reset >&2

log "benchmarking"
"$PY" -m lakehouse.trino_bench --days "$DAYS" --base "$BASE" --output "$OUT/results.json" >&2
