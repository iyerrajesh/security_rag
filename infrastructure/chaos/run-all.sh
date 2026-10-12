#!/usr/bin/env bash
# Run every backpressure / failure test and print a summary. Exit 1 if any fail.
# Each test wipes and recreates the local vehicle.* topics.
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
TESTS=(broker-pause producer-timeout small-queue broker-throttle slow-consumer consumer-restart)

declare -a results
failed=0
for t in "${TESTS[@]}"; do
  start=$SECONDS
  if "$DIR/$t.sh"; then r=PASS; else r=FAIL; failed=1; fi
  results+=("$(printf '%-18s %s  (%ss)' "$t" "$r" "$((SECONDS - start))")")
done

printf '\n== summary (details in output/chaos/<test>/) ==\n'
printf '%s\n' "${results[@]}"
exit "$failed"
