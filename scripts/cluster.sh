#!/usr/bin/env bash
# Local 3-broker cluster: ./scripts/cluster.sh start|stop|status|kill <broker-id>|restart <broker-id>
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=build/libs/minilog.jar
RUN=.cluster
JVM_FLAGS="-Xms512m -Xmx512m -XX:+UseG1GC"
mkdir -p "$RUN" data

build() { [ -f "$JAR" ] || ./gradlew -q jar; }

start_one() { # name mainclass config
  nohup java $JVM_FLAGS -cp "$JAR" "$2" "$3" > "$RUN/$1.log" 2>&1 &
  echo $! > "$RUN/$1.pid"
  echo "started $1 (pid $!), log: $RUN/$1.log"
}

stop_one() { # name signal
  if [ -f "$RUN/$1.pid" ]; then kill "-$2" "$(cat "$RUN/$1.pid")" 2>/dev/null || true; rm -f "$RUN/$1.pid"; fi
}

case "${1:-}" in
  start)
    build
    start_one controller dev.minilog.controller.ControllerMain config/controller.properties
    sleep 1
    for i in 1 2 3; do start_one "broker-$i" dev.minilog.broker.BrokerMain "config/broker-$i.properties"; done
    sleep 2
    java -Dminilog.log=quiet -cp "$JAR" dev.minilog.tools.Cli localhost:9090 describe ;;
  stop)
    for i in 1 2 3; do stop_one "broker-$i" TERM; done
    sleep 1; stop_one controller TERM ;;
  kill)    stop_one "broker-$2" KILL; echo "kill -9 broker-$2" ;;
  restart) start_one "broker-$2" dev.minilog.broker.BrokerMain "config/broker-$2.properties" ;;
  status)  java -Dminilog.log=quiet -cp "$JAR" dev.minilog.tools.Cli localhost:9090 describe ;;
  *) echo "usage: $0 start|stop|status|kill <id>|restart <id>"; exit 2 ;;
esac
