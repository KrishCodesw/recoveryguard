# Kafka Metadata Observer

## Cluster metadata

`ClusterInfoMain` uses Kafka Admin API `describeCluster()`. It reports the cluster ID, controller, and brokers.

## Topic and partition metadata

`TopicPartitionInfoMain` lists all topics, including internal topics, and describes each topic. It reports topic names, partition counts, leader IDs, replica IDs, and ISR IDs.

## Full metadata snapshot

`MetadataSnapshotMain` uses the Kafka Admin API to collect one cluster metadata snapshot. It collects:

- cluster ID and observation timestamp;
- broker IDs returned by Kafka;
- each topic partition's leader;
- assigned replica broker IDs; and
- ISR broker IDs.

Run against the Lab cluster:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.MetadataSnapshotMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

Use `--client-config <path>` for TLS or SASL properties. Use `--timeout-ms <100..120000>` to set the bounded request timeout.

The snapshot is collected through multiple Admin API requests. It is not an atomic broker-wide transaction. The observation timestamp is recorded after the metadata requests complete. Changes can occur during collection.

A broker missing from the returned broker list is not proof that its process is stopped. It means Kafka did not return that broker in this observation. A snapshot cannot show a transition that starts and ends between polls.

## Topic configuration metadata

`TopicConfigInfoMain` calls Kafka Admin API `describeConfigs()`. It reports config keys, values, sources, and sensitivity flags. Sensitive values are redacted.

## Safety boundary

All observer commands use Kafka Admin API only. They do not create consumers, join groups, read records, commit offsets, produce messages, create or delete topics, alter configurations, or change partition assignments.

Metadata does not prove record integrity, acknowledged-write durability, or application correctness. The snapshot collector does not issue recovery verdicts.
