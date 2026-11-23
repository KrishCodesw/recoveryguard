package com.recoveryguard.observer;

import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.PartitionMetadataSnapshot;
import com.recoveryguard.evidence.PartitionRef;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Collects one bounded Kafka metadata snapshot through the Admin API.
 *
 * <p>This collector does not create consumers, read records, join groups,
 * commit offsets, or change Kafka state.</p>
 */
public final class KafkaMetadataSnapshotCollector {
    private static final int MIN_TIMEOUT_MS = 100;
    private static final int MAX_TIMEOUT_MS = 120_000;
    private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    public ClusterMetadataSnapshot collect(Properties clientProperties, int timeoutMs)
            throws ExecutionException, InterruptedException, TimeoutException {

        if (clientProperties == null) {
            throw new IllegalArgumentException("clientProperties is required");
        }
        if (timeoutMs < MIN_TIMEOUT_MS || timeoutMs > MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException("timeoutMs must be between "
                    + MIN_TIMEOUT_MS + " and " + MAX_TIMEOUT_MS);
        }

        Properties properties = new Properties();
        properties.putAll(clientProperties);
        if (!properties.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
                || properties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG).isBlank()) {
            throw new IllegalArgumentException("bootstrap.servers is required");
        }
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "recoveryguard-metadata-snapshot");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(timeoutMs));
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.toString(timeoutMs));

        Admin admin = Admin.create(properties);
        try {
            DescribeClusterResult cluster = admin.describeCluster();
            String clusterId = cluster.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            Collection<Node> nodes = cluster.nodes().get(timeoutMs, TimeUnit.MILLISECONDS);

            List<String> topicNames = admin.listTopics(new ListTopicsOptions().listInternal(true))
                    .names().get(timeoutMs, TimeUnit.MILLISECONDS).stream()
                    .sorted()
                    .toList();

            Map<String, TopicDescription> descriptions = topicNames.isEmpty()
                    ? Map.of()
                    : admin.describeTopics(topicNames).allTopicNames()
                            .get(timeoutMs, TimeUnit.MILLISECONDS);

            return toSnapshot(clusterId, Instant.now(), nodes, descriptions);
        } finally {
            admin.close(ADMIN_CLOSE_TIMEOUT);
        }
    }

    static ClusterMetadataSnapshot toSnapshot(
            String clusterId,
            Instant observedAt,
            Collection<Node> nodes,
            Map<String, TopicDescription> descriptions) {

        if (nodes == null || descriptions == null) {
            throw new IllegalArgumentException("Kafka metadata collections are required");
        }

        Set<Integer> brokerIds = new LinkedHashSet<>();
        for (Node node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("broker metadata cannot contain null");
            }
            brokerIds.add(node.id());
        }

        Map<PartitionRef, PartitionMetadataSnapshot> partitions = new LinkedHashMap<>();
        descriptions.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    TopicDescription topic = entry.getValue();
                    if (topic == null) {
                        throw new IllegalArgumentException("topic description cannot be null");
                    }
                    for (TopicPartitionInfo partition : topic.partitions()) {
                        Set<Integer> replicas = partition.replicas().stream()
                                .map(Node::id)
                                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
                        Set<Integer> isr = partition.isr().stream()
                                .map(Node::id)
                                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
                        Integer leaderId = partition.leader() == null
                                ? null
                                : partition.leader().id();

                        PartitionRef reference = new PartitionRef(topic.name(), partition.partition());
                        PartitionMetadataSnapshot metadata =
                                new PartitionMetadataSnapshot(leaderId, replicas, isr);
                        if (partitions.put(reference, metadata) != null) {
                            throw new IllegalArgumentException(
                                    "duplicate partition metadata: " + reference);
                        }
                    }
                });

        return new ClusterMetadataSnapshot(clusterId, observedAt, brokerIds, partitions);
    }
}
