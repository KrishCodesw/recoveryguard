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

# Seed a deterministic fixture topic in Lab. This is test setup, not observer behavior.
MSYS_NO_PATHCONV=1 docker exec recoveryguard-lab-kafka-1-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create --if-not-exists \
  --topic recoveryguard-observer-check \
  --partitions 3 --replication-factor 3

topic_output="$(
  mvn --batch-mode --no-transfer-progress -q \
    -DskipTests exec:java \
    -Dexec.mainClass=com.recoveryguard.observer.TopicPartitionInfoMain \
    "-Dexec.args=--bootstrap-server ${bootstrap_servers}"
)"
printf '%s\n' "$topic_output"
grep -Eq '^topic.count=[1-9][0-9]*$' <<< "$topic_output"
grep -Fxq 'topic.name=recoveryguard-observer-check partition.count=3 internal=false' <<< "$topic_output"
grep -Eq '^partition.topic=recoveryguard-observer-check id=[0-2] leader=[123] replicas=[123,]+ isr=[123,]+$' <<< "$topic_output"

config_output="$(
  mvn --batch-mode --no-transfer-progress -q \
    -DskipTests exec:java \
    -Dexec.mainClass=com.recoveryguard.observer.TopicConfigInfoMain \
    "-Dexec.args=--bootstrap-server ${bootstrap_servers}"
)"
printf '%s\n' "$config_output"
grep -Eq '^topic.config.count=[1-9][0-9]*$' <<< "$config_output"
grep -Eq '^topic.config.entry topic=recoveryguard-observer-check key=min.insync.replicas value=.+ source=.+ sensitive=false$' <<< "$config_output"
grep -Eq '^topic.config.entry topic=recoveryguard-observer-check key=cleanup.policy value=.+ source=.+ sensitive=false$' <<< "$config_output"

snapshot_output="$(
  mvn --batch-mode --no-transfer-progress -q \
    -DskipTests exec:java \
    -Dexec.mainClass=com.recoveryguard.observer.MetadataSnapshotMain \
    "-Dexec.args=--bootstrap-server ${bootstrap_servers}"
)"
printf '%s\n' "$snapshot_output"
grep -Eq '^cluster.id=.+$' <<< "$snapshot_output"
grep -Eq '^observed.at=.+$' <<< "$snapshot_output"
grep -Fxq 'broker.count=3' <<< "$snapshot_output"
grep -Fxq 'partition.count=3' <<< "$snapshot_output"
grep -Eq '^partition.topic=recoveryguard-observer-check id=[0-2] leader=[123] replicas=[123,]+ isr=[123,]+$' <<< "$snapshot_output"

echo "Cluster, topic/partition, topic config, and full snapshot Lab checks passed."\n
# Exercise the complete product capture path against Lab. The database and report
# stay in the runner's temporary directory; no Lab code or ground truth is imported.
capture_dir="$(mktemp -d)"
trap 'rm -rf "$capture_dir"' EXIT
capture_output="$(
  mvn --batch-mode --no-transfer-progress -q \\
    -DskipTests exec:java \\
    -Dexec.mainClass=com.recoveryguard.observer.RecoveryCaptureMain \\
    "-Dexec.args=--bootstrap-server ${bootstrap_servers} --observations 3 --interval-ms 100 --database ${capture_dir}/evidence.db --report ${capture_dir}/report.json"
)"
printf '%s\\n' "$capture_output"
test -s "$capture_dir/evidence.db"
test -s "$capture_dir/report.json"
jq -e '
  .schemaVersion == 1
  and .status == "COMPLETED"
  and .observationCount == 3
  and .timelineEntryCount >= 3
  and .evidenceIntegrity == "VERIFIED"
  and .recordsRead == false
  and .consumerGroupJoined == false
  and .offsetsCommitted == false
  and .kafkaStateChanged == false
  and (.timeline | length) == .timelineEntryCount
' "$capture_dir/report.json" >/dev/null

echo "Recovery capture, SQLite persistence, integrity verification, and report checks passed."

