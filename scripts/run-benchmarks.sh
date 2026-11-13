#!/usr/bin/env bash
# Runs the benchmark suite used for BENCHMARK.md. Raw output goes to results/<name>.txt.
# Usage: ./scripts/run-benchmarks.sh [produce|consume|e2e|fsync|failover|chaos|all]
set -uo pipefail
cd "$(dirname "$0")/.."
./gradlew -q jar
JAR=build/libs/minilog.jar
BENCH_JVM="-Xms2g -Xmx2g -XX:+UseG1GC -Dminilog.log=quiet"
REPS=${REPS:-3}
mkdir -p results
bench() { # outfile args...
  local out=$1; shift
  echo "\$ java $BENCH_JVM -cp $JAR dev.minilog.bench.Bench $*" | tee -a "results/$out"
  java $BENCH_JVM -cp "$JAR" dev.minilog.bench.Bench "$@" 2>&1 | tee -a "results/$out"
}
header() { # outfile
  { echo "# $(date '+%Y-%m-%d %H:%M:%S %Z')  $(sysctl -n machdep.cpu.brand_string) $(sysctl -n hw.ncpu) cores $(sysctl -n hw.memsize | awk '{print $1/1073741824 " GiB"}')  $(sw_vers -productName) $(sw_vers -productVersion)";
    echo "# $(java -version 2>&1 | head -1)"; echo "# broker JVMs: -Xms512m -Xmx512m -XX:+UseG1GC ; all processes on localhost"; } >> "results/$1"
}
what=${1:-all}

if [[ $what == produce || $what == all ]]; then
  header produce.txt
  for rep in $(seq 1 "$REPS"); do
    for size in 100 1024; do
      if [ $size = 100 ]; then n=${PRODUCE_N_100:-1000000}; else n=${PRODUCE_N_1K:-300000}; fi
      for acks in 0 1 -1; do
        for batch in 16384 131072; do
          bench produce.txt produce --size $size --acks $acks --batch $batch --linger 5 --messages $n
        done
      done
    done
  done
fi
if [[ $what == consume || $what == all ]]; then
  header consume.txt
  for rep in $(seq 1 "$REPS"); do
    bench consume.txt consume --size 100 --messages ${CONSUME_N_100:-1000000}
    bench consume.txt consume --size 1024 --messages ${CONSUME_N_1K:-300000}
  done
fi
if [[ $what == e2e || $what == all ]]; then
  header e2e.txt
  for rep in $(seq 1 "$REPS"); do
    bench e2e.txt e2e --rate 5000 --seconds 20 --warmup-seconds 5 --acks 1 --size 100
    bench e2e.txt e2e --rate 5000 --seconds 20 --warmup-seconds 5 --acks -1 --size 100
  done
fi
if [[ $what == fsync || $what == all ]]; then
  header fsync.txt
  for rep in $(seq 1 "$REPS"); do
    for policy in none interval-1000ms every-1000 every-1; do
      bench fsync.txt fsync --policy $policy --size 1024 --acks 1 --batch 16384 --messages ${FSYNC_N:-100000}
    done
  done
fi
if [[ $what == long || $what == all ]]; then
  # Longer measurement windows for the headline configurations (the matrix runs above last 1-4 s each).
  header produce-long.txt
  header consume-long.txt
  for rep in $(seq 1 "$REPS"); do
    bench produce-long.txt produce --size 100 --acks -1 --batch 131072 --linger 5 --messages 10000000
    bench produce-long.txt produce --size 1024 --acks -1 --batch 131072 --linger 5 --messages 3000000
    bench consume-long.txt consume --size 100 --messages 5000000
    bench consume-long.txt consume --size 1024 --messages 2000000
  done
fi
if [[ $what == failover || $what == all ]]; then
  header failover.txt
  bench failover.txt failover --iterations 25 --session-ms 1500
  bench failover.txt failover --iterations 25 --session-ms 500
fi
if [[ $what == chaos || $what == all ]]; then
  header chaos.txt
  for seed in 20261112 7 42; do
    bench chaos.txt chaos --seconds ${CHAOS_SECONDS:-120} --seed $seed
  done
fi
