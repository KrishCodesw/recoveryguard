# Recovery Evidence Model

**Scope:** Metadata snapshots, change detection, timeline reconstruction, and persistence  
**Status:** Implemented Stage 3 evidence path

## 1. Purpose

This document defines the evidence that the current Observer collects and how RecoveryGuard records observed changes.

The current evidence model describes Kafka metadata. It does not prove record-level integrity, acknowledged-write durability, or application correctness.

## 2. Metadata snapshot

A cluster snapshot records the Kafka cluster ID, observation timestamp, broker IDs returned by Kafka, and partition metadata keyed by topic and partition.

Each partition entry records:

- leader broker ID, or no leader;
- assigned replica broker IDs; and
- in-sync replica (ISR) broker IDs.

The snapshot model is immutable. The ISR set must be a subset of the assigned replica set. A leader, when present, must be one of the assigned replicas.

A snapshot uses multiple Admin API requests. It is not an atomic view of all brokers and partitions.

## 3. Change detection

`RecoveryChangeDetector` is a pure Java component. It compares two snapshots from the same cluster.

| Signal | Detection rule |
|---|---|
| `BROKER_UNAVAILABLE` | Broker ID exists in the previous broker set but not in the current set |
| `BROKER_AVAILABLE` | Broker ID exists in the current broker set but not in the previous set |
| `LEADER_CHANGED` | A partition exists in both snapshots and its leader ID changed |
| `ISR_CHANGED` | A partition exists in both snapshots and its ISR set changed |

Each event uses the current snapshot timestamp. Broker events include the broker ID and partitions assigned to that broker in either snapshot. Leader events include previous and current leader IDs. ISR events include broker IDs added to or removed from the ISR.

### Detection limits

- Broker membership is based on metadata returned by Kafka. It is not a process-health check.
- Two observations cannot detect a transition that occurs and reverses between them.
- A partition created or deleted between snapshots is not treated as a leader or ISR transition.
- A missing partition is not compared.
- The detector does not infer data loss or recovery correctness.
- The detector does not create a verdict.
- Both snapshots must identify the same cluster and have valid timestamp order.

## 4. Timeline reconstruction

`RecoveryTimelineReconstructor` builds an immutable timeline from an ordered list of snapshots.

The timeline contains one entry for each snapshot, detected broker and partition events, a contiguous sequence number, input observation index, observation timestamp, and ordering confidence.

### Ordering rules

1. All snapshots must identify the same cluster.
2. Input timestamps must be non-decreasing.
3. Input observation order is retained.
4. A snapshot entry precedes events detected from that snapshot.
5. Events from one observation use stable ordering: broker unavailable, broker available, leader changed, then ISR changed. Entity IDs provide tie-breaking.
6. Events from one observation use `SAME_OBSERVATION`. This is not proof of causality.
7. Observations with equal timestamps use `UNKNOWN_ORDER`. Input order is retained only for deterministic output.
8. `observedAt` is copied from the snapshot. `occurredAt` remains absent unless a trusted source supplies a transition time.
9. The reconstructor does not invent intermediate states or unobserved transitions.

| Ordering value | Meaning |
|---|---|
| `OBSERVED_ORDER` | Observation timestamp is unique in the timeline |
| `SAME_OBSERVATION` | Event was detected from the same observation as other entries |
| `UNKNOWN_ORDER` | Multiple observations share the same timestamp |

A sequence number gives each entry a stable position. It does not establish causality.

## 5. SQLite persistence

`SQLiteEvidenceStore` stores evidence in a local SQLite database.

| Table | Contents |
|---|---|
| `investigations` | Investigation ID, Kafka cluster ID, lifecycle status, timestamps, schema version, and RecoveryGuard version |
| `observations` | Raw metadata snapshots in collection order |
| `timeline_entries` | Reconstructed timeline entries linked to source observations |

Observation indexes start at zero and must be contiguous. Timeline rows must link to an observation from the same investigation.

### Lifecycle

1. Create an investigation with `IN_PROGRESS`.
2. Append each snapshot in its own transaction.
3. Reconstruct the timeline from stored observations.
4. Persist timeline entries in one transaction.
5. Verify evidence hashes.
6. Mark the investigation `COMPLETED`.

At startup, `recoverInterruptedInvestigations()` changes remaining `IN_PROGRESS` investigations to `INTERRUPTED`. Interrupted investigations are not resumed. A new investigation receives a new UUID.

### Integrity and write rules

- Observation and timeline rows are append-only through SQLite triggers.
- Evidence hashes cover payloads and associated observation or timeline metadata.
- Evidence reads verify hashes before returning data.
- Timeline persistence is atomic.
- Completion verifies evidence in the transaction that changes investigation status.
- SHA-256 detects changes when the stored hash is not also changed.
- SHA-256 does not protect against an actor who can rewrite both evidence and hashes.

The implementation uses SQLite WAL mode, full synchronous writes, and a five-second busy timeout. It targets one RecoveryGuard process per database file. It is not a distributed database service.

## 6. Evidence integrity is not data integrity

| Term | Meaning |
|---|---|
| Evidence integrity | Stored RecoveryGuard evidence matches its stored hashes |
| Source authenticity | Evidence came from the source it claims to represent |
| Kafka record integrity | Kafka records match an independent record-level expectation |
| Application correctness | The application preserved its defined business invariants |

The current product verifies stored evidence hashes. The Lab verifier tests record preservation in controlled experiments. A customer-facing record-integrity check requires an approved application evidence contract.

## 7. Lab validation

The Lab provides an independent acknowledgement ledger and record verifier. The product integration workflow checks evidence capture while the Lab injects a controlled leader failure.

An RG-00 result applies only to its recorded topology, configuration, workload, and failure sequence. It does not prove that all Kafka failure modes are safe.

## 8. Tests

Product tests cover snapshot validation, broker changes, leader changes, ISR changes, unchanged metadata, partition creation and deletion boundaries, cluster identity, timestamp order, timeline ordering, SQLite persistence, lifecycle transitions, and evidence hash verification.
