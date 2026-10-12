# Shared helpers for the chaos scripts. Source it; don't run it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
PY="$ROOT/.venv/bin/python"
CHAOS_DIR="$ROOT/infrastructure/chaos"
TEST_NAME="$(basename "$0" .sh)"
OUT="$ROOT/output/chaos/$TEST_NAME"

log() { printf '\n\033[1m[%s] %s\033[0m\n' "$TEST_NAME" "$*" >&2; }

# docker compose with the platform profile; hides the unrelated "variable is not set" noise
dc() { docker compose --profile m1 "$@" 2> >(grep -v -E 'variable is not set|^ Container ' >&2); }
kafka_bin() { dc exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server kafka:9092 "${@:2}"; }

setup() {
  [[ -x "$PY" ]] || "$ROOT/simulator/run.sh" --help >/dev/null  # creates .venv on first use
  rm -rf "$OUT" && mkdir -p "$OUT"
  dc unpause kafka >/dev/null 2>&1 || true
  dc up -d --wait kafka >/dev/null
}

# Delete and recreate the vehicle topics so each test starts from empty topics.
# This wipes local Kafka data (simulator output only).
reset_topics() {
  log "resetting vehicle.* topics"
  kafka_bin kafka-topics.sh --delete --topic 'vehicle\..*' >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    [[ -z "$(kafka_bin kafka-topics.sh --list | grep '^vehicle\.' || true)" ]] && break
    sleep 1
  done
  dc run --rm kafka-init >/dev/null
}

# simulate <run-name> [simulator flags...]  -> $OUT/<run-name>/manifest.json
simulate() {
  local name="$1"; shift
  "$PY" -m simulator --sink kafka --output "$OUT/$name" "$@"
}

# consume <group-id> <summary-file> [consumer flags...]
consume() {
  local group="$1" summary="$2"; shift 2
  # verify.py decides pass/fail from the summary, so a non-zero exit here must not abort the script
  "$PY" -m ingestion.consume --from-beginning --group-id "$group" --summary-json "$summary" "$@" || true
}

# Total committed lag of a consumer group (empty if the group has not committed yet).
group_lag() {
  kafka_bin kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ { lag += $6; seen = 1 } END { if (seen) print lag }'
}

# Total committed offset of a consumer group.
group_committed() {
  kafka_bin kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null \
    | awk '$4 ~ /^[0-9]+$/ { off += $4 } END { print off + 0 }'
}

verify() { "$PY" "$CHAOS_DIR/verify.py" "$TEST_NAME" "$@" | tee "$OUT/result.txt"; }
