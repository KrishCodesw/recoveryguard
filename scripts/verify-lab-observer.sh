#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"

output="$(
  mvn --batch-mode --no-transfer-progress -q \
    -DskipTests \
    exec:java \
    -Dexec.mainClass=com.recoveryguard.observer.ClusterInfoMain \
    "-Dexec.args=--bootstrap-server ${bootstrap_servers}"
)"

printf '%s\n' "$output"

grep -Eq '^cluster.id=.+$' <<< "$output"
grep -Eq '^controller.id=[123]$' <<< "$output"
grep -Fxq 'broker.count=3' <<< "$output"

for broker_id in 1 2 3; do
  grep -Eq "^broker.id=${broker_id} " <<< "$output"
done

echo "Observer Lab integration check passed."
