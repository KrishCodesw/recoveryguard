# RecoveryGuard Glossary

| Term | Definition |
|---|---|
| Acknowledged write | Producer write for which Kafka returned a successful acknowledgement under the configured acknowledgement policy |
| Admin API | Kafka client API used to inspect and administer cluster resources |
| Baseline | Evidence collected before an incident or failure |
| Broker | Kafka server that stores and serves partition replicas |
| Bootstrap server | Initial Kafka endpoint used to discover cluster metadata and broker endpoints |
| Collection run | One bounded execution of an evidence collection plan |
| Consumer group | Kafka mechanism that coordinates consumers that share topic partitions |
| Evidence | Timestamped observation or independent source record used by a validation check |
| Evidence bundle | Defined set of evidence, metadata, hashes, check results, and report information for an investigation |
| Evidence completeness | Whether all evidence required by a check is present |
| Evidence integrity | Whether stored evidence matches its stored integrity hashes |
| Evidence provenance | Information about the source and collection process for an evidence item |
| Evidence freshness | Whether evidence is recent enough for a check's stated time window |
| Ground truth | Independently established expected outcome used to evaluate RecoveryGuard in the Lab |
| High watermark (HW) | Replication boundary used by Kafka to identify records replicated to all current in-sync replicas |
| Incident window | Time range used to group evidence related to a recovery event |
| In-sync replica (ISR) | Replica that Kafka currently considers in sync for a partition |
| Leader | Broker that serves reads and writes for a partition |
| Log end offset (LEO) | Offset of the next record in a replica's log |
| Offset | Record position within a Kafka partition; not a universal count of business events |
| Observer effect | Change in target-system behaviour caused by Observer requests or resource use |
| Partition | Ordered log unit within a Kafka topic |
| Recovery | Kafka or platform action that restores service after a failure |
| Recovery correctness | Whether evidence satisfies explicit validation conditions for a stated scope |
| Recovery timeline | Ordered representation of observed snapshots and detected changes |
| Required check | Check whose outcome is required for the configured validation decision |
| Optional check | Check that adds information but does not block a verdict by itself |
| Snapshot | Bounded observation of Kafka metadata at a recorded observation time |
| Source authenticity | Confidence that evidence came from the source it claims to represent |
| Verdict | Result produced by validation policy for a defined evidence set and scope |
| Validation scope | Exact cluster, resources, time window, checks, and evidence classes covered by a verdict |

## Interpretation rules

- Successful broker recovery does not prove that all acknowledged writes survived.
- An offset gap does not automatically prove data loss.
- A missing baseline can prevent a data-preservation claim.
- Evidence hashes do not prove source authenticity.
- Metadata evidence does not prove application correctness.
- A verdict applies only to its stated scope, checks, evidence, and versions.
- A Lab result applies only to the topology, configuration, workload, and failure scenario recorded for that run.
