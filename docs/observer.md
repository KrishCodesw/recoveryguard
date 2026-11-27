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


## Bounded evidence capture

`RecoveryCaptureMain` runs a bounded metadata capture. It collects multiple snapshots through the Admin API, stores each snapshot in SQLite, reconstructs the timeline, verifies the stored hashes, and completes the investigation.

Example:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.RecoveryCaptureMain \
  '-Dexec.args=--bootstrap-server localhost:29092 --observations 20 --interval-ms 1000 --database ./data/recoveryguard.db --report ./reports/investigation.json'
```

Options:

- `--bootstrap-server`: required Kafka bootstrap endpoints.
- `--client-config`: optional Kafka client properties for TLS or SASL.
- `--observations`: number of snapshots; default 5, range 2–10000.
- `--interval-ms`: delay between snapshots; default 1000, range 0–600000.
- `--timeout-ms`: bounded Admin API request timeout; default 10000, range 100–120000.
- `--database`: SQLite file; default `recoveryguard-evidence.db`.
- `--report`: optional JSON report path. Without it, JSON is written to standard output.

The command calls `recoverInterruptedInvestigations()` before it starts a new investigation. If capture stops before completion, the investigation remains `IN_PROGRESS`. The next startup marks it `INTERRUPTED`; it does not resume or change its evidence.

The report contains the investigation ID, cluster ID, capture counts, detected metadata signals, ordered timeline entries, evidence-integrity status, and explicit observer safety flags. The report does not claim record-level integrity. Use an independent application evidence source for that claim.

The report's `evidenceIntegrity=VERIFIED` value means that the stored observation and timeline hashes passed the SQLite integrity checks. It does not prove that Kafka data is correct or that the evidence source is authentic.

The Lab observer integration workflow tests:
- the capture command against the three-broker Lab;
- SQLite persistence and evidence integrity;
- repeatability of the stable metadata projection; and
- detection of a leader change while the Lab injects a controlled leader failure.

The Lab recorder remains a separate test tool. It is not a product dependency.
