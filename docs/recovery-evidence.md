# Recovery Evidence: Metadata Change Detection

## Purpose

Stage 3.2 compares two timestamped Kafka metadata snapshots. It emits events for broker membership, partition leader, and ISR changes.

The detector is a pure Java component. It does not connect to Kafka. A later collector will create snapshots through the Kafka Admin API.

## Input

Each cluster snapshot contains:

- Kafka cluster ID;
- observation timestamp;
- broker IDs returned by the cluster metadata query; and
- partition metadata keyed by topic and partition.

Each partition entry contains:

- leader broker ID, or no leader;
- assigned replica broker IDs; and
- in-sync replica (ISR) broker IDs.

The snapshot types are immutable. The ISR set must be a subset of the replica set. A leader, when present, must be one of the assigned replicas.

## Detected events

| Signal | Detection rule |
| --- | --- |
| BROKER_UNAVAILABLE | A broker ID exists in the previous broker set but not in the current broker set |
| BROKER_AVAILABLE | A broker ID exists in the current broker set but not in the previous broker set |
| LEADER_CHANGED | A partition exists in both snapshots and its leader ID changed |
| ISR_CHANGED | A partition exists in both snapshots and its ISR set changed |

Every event uses the current snapshot timestamp.

Broker events include the broker ID and partitions assigned to that broker in either snapshot. Leader events include the old and new leader IDs. ISR events include the broker IDs added to or removed from the ISR.

## Important limits

- Broker availability means presence or absence in the broker list returned by Kafka metadata. It is not a direct process-health check.
- The detector compares two observations. It cannot identify changes that occur and reverse between those observations.
- A new or deleted partition is not treated as a leader or ISR transition.
- A missing partition in either snapshot is not compared.
- The detector does not infer data loss, acknowledged-write loss, or recovery correctness.
- The detector does not create a verdict.
- Snapshot timestamps must be ordered and both snapshots must identify the same Kafka cluster.

## Tests

RecoveryChangeDetectorTest checks broker changes, leader changes, ISR changes, unchanged metadata, partition creation/deletion boundaries, cluster identity, timestamp order, and leader loss.


## Stage 3.4: Recovery timeline reconstruction

`RecoveryTimelineReconstructor` builds an immutable timeline from an ordered list of metadata snapshots.

The timeline contains:

- one snapshot entry for every input observation, including observations with no detected changes;
- broker events for broker membership changes;
- partition events for leader and ISR changes;
- a contiguous sequence number for deterministic display and processing;
- the input observation index and observation timestamp; and
- an ordering-confidence value.

### Ordering rules

1. Input snapshots must belong to the same cluster.
2. Input timestamps must be in non-decreasing order.
3. Entries follow observation order. The reconstructor does not sort observations by a generated event timestamp.
4. Each observation's snapshot entry comes before events detected from that observation.
5. Events from one observation use stable ordering: broker unavailable, broker available, leader changed, then ISR changed. Entity IDs provide tie-breaking.
6. Events from the same observation are marked `SAME_OBSERVATION`. This is a display order, not proof of causality.
7. If two or more observations have the same timestamp, their entries are marked `UNKNOWN_ORDER`. Input order is retained only to make output deterministic.
8. `observedAt` is copied from the snapshot. `occurredAt` remains absent unless a trusted source supplies an actual transition time.
9. The reconstructor does not infer intermediate states or transitions that occurred and reversed between snapshots.

### Timeline ordering confidence

| Value | Meaning |
| --- | --- |
| `OBSERVED_ORDER` | The observation timestamp is unique in this timeline |
| `SAME_OBSERVATION` | The event was detected from the same observation as other entries |
| `UNKNOWN_ORDER` | Multiple observations share the same timestamp |

The sequence number gives every entry a stable position. It does not add temporal or causal evidence.

The reconstructor is a pure component. It does not connect to Kafka, persist evidence, or create a verdict.

The reconstructor uses only snapshots supplied by its caller. It does not schedule polling, create missing observations, or fill gaps in collection.

## Stage 3.5: SQLite evidence persistence

`SQLiteEvidenceStore` stores evidence in one SQLite database file per RecoveryGuard installation.

### Storage model

- `investigations` stores the investigation ID, Kafka cluster ID, lifecycle status, timestamps, schema version, and RecoveryGuard version.
- `observations` stores every raw metadata snapshot in collection order. Observation indexes start at zero and must be contiguous.
- `timeline_entries` stores the reconstructed timeline order and event payloads. Snapshot entries refer to their original observation and do not duplicate the snapshot payload.

The database can contain multiple investigations for multiple Kafka clusters. Every timeline row is linked to an observation from the same investigation.

### Lifecycle

1. Create an investigation with status `IN_PROGRESS`.
2. Append each raw snapshot in its own transaction.
3. Reconstruct the timeline from the stored observations.
4. Persist all timeline entries in one transaction.
5. Verify evidence hashes.
6. Mark the investigation `COMPLETED`.

At application startup, call `recoverInterruptedInvestigations()` once before accepting new work. It changes all remaining `IN_PROGRESS` investigations to `INTERRUPTED`. Interrupted investigations are not resumed. A new investigation receives a new UUID.

### Integrity and write rules

- Observation and timeline rows are append-only. SQLite triggers reject update and delete operations.
- Each evidence hash covers the payload and its associated timeline or observation metadata.
- Evidence reads verify hashes before returning data.
- Timeline persistence is atomic. A failed write rolls back the complete timeline transaction.
- Completion verifies evidence inside the same SQLite write transaction that changes the investigation status.
- SHA-256 detects accidental or unauthorised changes when the stored hash is not also changed. It does not provide protection against an actor who can modify both evidence and hashes.

### Deployment notes

SQLite is embedded. No separate database server is required. The database file remains inside the customer's environment. The current implementation uses WAL mode, full synchronous writes, and a five-second SQLite busy timeout. This design targets one RecoveryGuard application process per database file.

### Scope

The persistence layer stores the existing Stage 3.1–3.4 evidence model. Topic configuration data remains outside `ClusterMetadataSnapshot` and is not included in this persistence scope.