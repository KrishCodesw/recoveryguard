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
