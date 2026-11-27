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

echo "Recovery capture, SQLite persistence, integrity verification, and report checks passed."\n
# Repeat a stable capture. Compare the stable projection, not UUIDs or timestamps.
mvn --batch-mode --no-transfer-progress -q \\
  -DskipTests exec:java \\
  -Dexec.mainClass=com.recoveryguard.observer.RecoveryCaptureMain \\
  "-Dexec.args=--bootstrap-server ${bootstrap_servers} --observations 3 --interval-ms 100 --database ${capture_dir}/repeat.db --report ${capture_dir}/repeat.json"
jq -S '{
  clusterId,
  observationCount,
  timelineEntryCount,
  initialBrokerCount,
  finalBrokerCount,
  initialPartitionCount,
  finalPartitionCount,
  detectedSignalCounts,
  timeline: [.timeline[] | {
    type, signal, observedBrokerIds, affectedBrokerIds, affectedPartitions
  }]
}' "$capture_dir/report.json" > "$capture_dir/first-stable.json"
jq -S '{
  clusterId,
  observationCount,
  timelineEntryCount,
  initialBrokerCount,
  finalBrokerCount,
  initialPartitionCount,
  finalPartitionCount,
  detectedSignalCounts,
  timeline: [.timeline[] | {
    type, signal, observedBrokerIds, affectedBrokerIds, affectedPartitions
  }]
}' "$capture_dir/repeat.json" > "$capture_dir/second-stable.json"
cmp "$capture_dir/first-stable.json" "$capture_dir/second-stable.json"

# Run a controlled leader failure while RecoveryGuard polls metadata.
lab_dir=".ci/recoveryguard-lab"
failure_topic="recoveryguard-observer-recovery-check"
failure_run="observer-integration-rg00"
(
  cd "$lab_dir"
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \\
    --bootstrap-server kafka-1:19092 \\
    --create --if-not-exists \\
    --topic "$failure_topic" --partitions 1 --replication-factor 3 \\
    --config min.insync.replicas=2 \\
    --config cleanup.policy=delete \\
    --config retention.ms=86400000
  java -jar ground-truth/target/ground-truth-0.1.0-SNAPSHOT.jar create \\
    --output-dir "$capture_dir/lab-runs" \\
    --run-id "$failure_run" --topic "$failure_topic" \\
    --bootstrap-servers localhost:29092 \\
    --description "Observer integration leader failure"
)
mvn --batch-mode --no-transfer-progress -q \\
  -DskipTests exec:java \\
  -Dexec.mainClass=com.recoveryguard.observer.RecoveryCaptureMain \\
  "-Dexec.args=--bootstrap-server ${bootstrap_servers} --observations 60 --interval-ms 500 --database ${capture_dir}/failure.db --report ${capture_dir}/failure.json" \\
  > "$capture_dir/failure-capture.log" 2>&1 &
capture_pid=$!
failure_status=0
(
  cd "$lab_dir"
  bash scripts/rg-00-leader-failure.sh \
    "$failure_run" "$failure_topic" "$capture_dir/lab-runs"
) || failure_status=$?
wait "$capture_pid"
if [[ "$failure_status" -ne 0 ]]; then
  cat "$capture_dir/failure-capture.log"
  echo "Controlled Lab leader failure did not complete." >&2
  exit "$failure_status"
fi
jq -e '
  .status == "COMPLETED"
  and .evidenceIntegrity == "VERIFIED"
  and ([.timeline[] | select(.signal == "LEADER_CHANGED")] | length) >= 1
' "$capture_dir/failure.json" >/dev/null

echo "Repeatability and controlled leader-failure observation checks passed."


