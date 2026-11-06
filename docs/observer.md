# Kafka Metadata Observer

## Cluster metadata

`ClusterInfoMain` uses Kafka Admin API `describeCluster()`. It reports the cluster ID, controller, and brokers.

## Topic and partition metadata

`TopicPartitionInfoMain` lists all topics, including internal topics, and describes each topic. It reports:

- topic name, partition count, and internal-topic flag;
- partition ID and leader broker ID;
- replica broker IDs; and
- in-sync replica (ISR) broker IDs.

Topics and partitions are sorted for stable output. Broker ID lists are sorted. A leader value of `none` means Kafka returned no leader for that partition. This is a metadata snapshot. It does not prove record or application-data integrity.

Run against the Lab cluster:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.TopicPartitionInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

Example output (values depend on the cluster):

```text
topic.count=1
topic.name=orders partition.count=3 internal=false
partition.topic=orders id=0 leader=1 replicas=1,2,3 isr=1,2,3
partition.topic=orders id=1 leader=2 replicas=1,2,3 isr=1,2,3
partition.topic=orders id=2 leader=3 replicas=1,2,3 isr=1,2,3
```

## Client security settings and timeout

Use `--client-config <path>` for Kafka client properties, including TLS and SASL settings. The explicit bootstrap endpoint overrides the properties file. The default timeout is 10 seconds; accepted values are 100 to 120000 ms.

## Safety boundary

Both commands use Kafka Admin API only. They do not create consumers, join groups, read records, commit offsets, produce messages, create/delete topics, alter configurations, or change partition assignments.

The observer is read-only. It does not execute recovery or issue a recovery verdict. Metadata can show leadership and ISR state, but cannot prove application-level correctness or acknowledged-write durability.
