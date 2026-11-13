#!/usr/bin/env bash
# ./scripts/cli.sh create-topic NAME P RF | describe | produce TOPIC | consume TOPIC GROUP
cd "$(dirname "$0")/.."
[ -f build/libs/minilog.jar ] || ./gradlew -q jar
exec java -Dminilog.log=quiet -cp build/libs/minilog.jar dev.minilog.tools.Cli "${BOOTSTRAP:-localhost:9090}" "$@"
