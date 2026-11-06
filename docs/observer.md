# Kafka Cluster Observer

## Current scope

`ClusterInfoMain` is the first observer command. It connects to a Kafka bootstrap endpoint and calls only `Admin.describeCluster()`.

It reports:

- cluster ID;
- controller node ID;
- broker count; and
- broker IDs, advertised hosts, ports, and rack values.

It does not list topics, read offsets, inspect consumer groups, read records, or collect partition and ISR state. Those operations are not part of this change.

## Safety boundary

The command uses the Kafka Admin client only.

It does not:

- construct a Kafka Consumer;
- subscribe to or assign topic partitions;
- poll records;
- join consumer groups;
- read or commit group offsets;
- produce records;
- create or delete topics;
- alter topic or broker configuration; or
- change partition assignments or elect leaders.

The current implementation makes one Admin API call: `describeCluster()`. Broker metadata is sorted by broker ID before it is printed.

## Run against the Lab cluster

Start the three-broker cluster by following the instructions in the `recoveryguard-lab` README. Then run this command from the product repository:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.ClusterInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

The output includes lines similar to:

```text
cluster.id=<cluster-id>
controller.id=1
broker.count=3
broker.id=1 host=localhost port=29092 rack=unknown
broker.id=2 host=localhost port=39092 rack=unknown
broker.id=3 host=localhost port=49092 rack=unknown
```

The controller ID can change after a controller election. Broker IDs and ports in the example match the Lab cluster.

## Client security settings

Use `--client-config <path>` to load standard Kafka client properties from a local file. This supports client security settings such as TLS and SASL without putting secret values in command arguments.

The explicit `--bootstrap-server` argument takes precedence over a bootstrap address in the properties file. The command does not print the properties file or its values.

Example:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.ClusterInfoMain \
  '-Dexec.args=--bootstrap-server broker-1.example:9093 --client-config ./kafka-client.properties'
```

The request timeout defaults to 10 seconds. Set `--timeout-ms` to a value from 100 to 120000 when required.

## Integration verification

The `observer-integration.yml` workflow checks out the Lab infrastructure at a fixed commit:

`16d61c8f4825340145d524f26969353e0dd1422f`

It starts the three-broker cluster, runs Maven tests, executes `ClusterInfoMain`, and checks that broker IDs 1, 2, and 3 are reported.

This is a CI test fixture only. The product build, runtime, and package do not include the Lab repository or its files. The pinned Lab commit changes only through an approved product PR.

## Limits

A successful result proves only that the Admin client could obtain cluster metadata at the time of the call. It is not a recovery verdict and does not prove topic, partition, ISR, offset, or application-data integrity.
