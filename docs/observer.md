# Kafka Observer Operations

**Component:** RecoveryGuard Observer  
**Interface:** Java command-line tools  
**Access model:** Kafka Admin API only

## 1. Purpose

The Observer collects Kafka metadata for recovery investigations. It does not perform recovery actions and does not read Kafka records.

## 2. Requirements

- JDK 17
- Maven 3.9 or later
- Network access to the Kafka bootstrap endpoint
- Network access to broker endpoints returned by Kafka metadata
- Kafka permissions for the requested metadata operations

The client must resolve and reach advertised broker endpoints. A reachable bootstrap endpoint does not prove that all broker endpoints are reachable.

## 3. Commands

### Cluster metadata

`ClusterInfoMain` calls `describeCluster()` and reports the cluster ID, controller, and broker endpoints.

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.ClusterInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

### Topic and partition metadata

`TopicPartitionInfoMain` lists topics and describes partitions. It reports partition counts, leaders, assigned replicas, and ISR membership.

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.TopicPartitionInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

### Topic configuration metadata

`TopicConfigInfoMain` calls `describeConfigs()`. It reports configuration keys, values, sources, and sensitivity flags. Sensitive values are redacted.

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.TopicConfigInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

### Metadata snapshot

`MetadataSnapshotMain` collects cluster identity, observation timestamp, broker IDs, partition leaders, assigned replicas, and ISR membership.

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.MetadataSnapshotMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

### Bounded evidence capture

`RecoveryCaptureMain` collects multiple snapshots, persists them in SQLite, reconstructs the timeline, verifies stored hashes, completes the investigation, and writes a JSON report.

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.RecoveryCaptureMain \
  '-Dexec.args=--bootstrap-server localhost:29092 --observations 20 --interval-ms 1000 --database ./data/recoveryguard.db --report ./reports/investigation.json'
```

| Option | Description | Default / range |
|---|---|---|
| `--bootstrap-server` | Kafka bootstrap endpoints | Required |
| `--client-config` | Kafka client properties file | Not set |
| `--observations` | Number of snapshots | 5; 2–10000 |
| `--interval-ms` | Delay between snapshots | 1000; 0–600000 |
| `--timeout-ms` | Admin API request timeout | 10000; 100–120000 |
| `--database` | SQLite database path | `recoveryguard-evidence.db` |
| `--report` | JSON report path | Standard output |

## 4. TLS and SASL

Use `--client-config <path>` to supply Kafka client properties.

Example structure:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=<secret supplied by deployment>
ssl.truststore.location=<path>
ssl.truststore.password=<secret supplied by deployment>
```

This is a property example, not a secret-management solution. Do not commit credentials, private keys, passwords, or customer certificates. Use customer-controlled secret storage and suitable file permissions.

## 5. Capture lifecycle

The capture command calls `recoverInterruptedInvestigations()` before it starts a new investigation.

1. A new investigation starts as `IN_PROGRESS`.
2. Each snapshot is appended as an observation.
3. The timeline is reconstructed and persisted.
4. Evidence hashes are verified.
5. A successful capture is marked `COMPLETED`.
6. An investigation still marked `IN_PROGRESS` at the next startup is marked `INTERRUPTED`.

Interrupted investigations are not resumed. Start a new investigation to collect new observations.

## 6. Report interpretation

The report includes investigation identity, cluster identity, capture counts, detected signals, timeline entries, evidence-integrity status, and Observer safety flags.

`evidenceIntegrity=VERIFIED` means that stored observation and timeline hashes passed the SQLite integrity checks. It does not prove source authenticity, Kafka record preservation, or application correctness.

## 7. Snapshot and timeline limits

- A snapshot uses multiple Admin API requests. It is not an atomic cluster-wide transaction.
- The observation timestamp is recorded after the metadata requests complete.
- A broker missing from the returned broker list is not proof that its process is stopped.
- A transition that starts and ends between polls may not be observed.
- Events from one observation have no proven causal order.
- Equal observation timestamps have unknown order.
- A timeline sequence number provides stable display order, not causality.

## 8. Observer safety contract

The Observer must not create a Kafka Consumer, subscribe or assign partitions, poll records, join groups, commit offsets, produce records, create or delete topics, alter configuration, change assignments, or trigger leader election.

Every new Admin API call requires a review of permissions, request cost, timeout, polling frequency, and failure behaviour.

## 9. Operational security

- Use a dedicated least-privilege Kafka identity.
- Protect client configuration and evidence files.
- Do not store secrets in command history or logs.
- Keep the database outside the monitored Kafka cluster.
- Restrict access to investigation reports.
- Use TLS and approved authentication outside local test environments.
- Monitor disk capacity for database and report output.
- Do not expose Kafka endpoints to untrusted networks.

## 10. Troubleshooting

| Symptom | Check |
|---|---|
| Bootstrap connection fails | Endpoint, route, listener, and security protocol |
| Metadata call times out | Broker reachability and advertised listeners |
| Authorization error | Kafka ACLs for the requested Admin API operations |
| Partial metadata | Broker availability, request errors, and collection timing |
| Investigation is `INTERRUPTED` | Process termination; start a new investigation |
| Hash verification fails | Preserve the database and investigate storage or evidence changes |
| Data loss is suspected despite `VERIFIED` | Obtain independent record-level evidence |

## 11. Integration test

Product CI runs Observer integration tests against a pinned Lab revision. The test checks bounded capture, SQLite persistence, hash verification, repeatability of stable metadata, and detection of a controlled leader change.

The Lab is a test fixture, not a product runtime dependency.
