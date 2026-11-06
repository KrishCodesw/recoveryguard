#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"

cluster_output="$(
  mvn --batch-mode --no-transfer-progress -q \
    -DskipTests exec:java \
    -Dexec.mainClass=com.recoveryguard.observer.ClusterInfoMain \
    "-Dexec.args=--bootstrap-server ${bootstrap_servers}"
)"
printf '%s\n' "$cluster_output"
grep -Eq '^cluster.id=.+$' <<< "$cluster_output"
grep -Eq '^controller.id=[123]$' <<< "$cluster_output"
grep -Fxq 'broker.count=3' <<< "$cluster_output"
for broker_id in 1 2 3; do
  grep -Eq "^broker.id=${broker_id} " <<< "$cluster_output"
done

topic_output="$(
  mvn --batch-mode --no-transfer-progress -q \
    -DskipTests exec:java \
    -Dexec.mainClass=com.recoveryguard.observer.TopicPartitionInfoMain \
    "-Dexec.args=--bootstrap-server ${bootstrap_servers}"
)"
printf '%s\n' "$topic_output"
grep -Eq '^topic.count=[1-9][0-9]*$' <<< "$topic_output"
grep -Eq '^topic.name=.+ partition.count=[1-9][0-9]* internal=(true|false)$' <<< "$topic_output"
grep -Eq '^partition.topic=.+ id=[0-9]+ leader=[0-9]+ replicas=[0-9,]+ isr=[0-9,]+$' <<< "$topic_output"

echo "Cluster and topic/partition observer Lab integration checks passed."
