#!/usr/bin/env bash
# Runs the full test suite N times (default 10) with --rerun-tasks and records pass/fail per run.
set -u
cd "$(dirname "$0")/.."
N=${1:-10}
OUT=results/test-runs
mkdir -p "$OUT"
pass=0
for i in $(seq 1 "$N"); do
  start=$(date +%s)
  if ./gradlew test --rerun-tasks -q > "$OUT/run-$i.log" 2>&1; then
    status=PASS; pass=$((pass+1))
  else
    status=FAIL
  fi
  tests=$(grep -ho 'tests="[0-9]*"' build/test-results/test/*.xml | grep -o '[0-9]*' | paste -sd+ - | bc)
  failures=$(grep -ho 'failures="[0-9]*"' build/test-results/test/*.xml | grep -o '[0-9]*' | paste -sd+ - | bc)
  echo "run $i: $status tests=$tests failures=$failures seconds=$(( $(date +%s) - start ))" | tee -a "$OUT/summary.txt"
  if [ "$status" = FAIL ]; then mkdir -p "$OUT/fail-$i" && cp build/test-results/test/*.xml "$OUT/fail-$i/"; fi
done
echo "PASSED $pass / $N runs" | tee -a "$OUT/summary.txt"
