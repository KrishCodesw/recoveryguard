package com.recoveryguard.observer;

import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.PartitionMetadataSnapshot;
import com.recoveryguard.evidence.PartitionRef;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMetadataSnapshotCollectorTest {

    private final Instant observedAt = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    void mapsKafkaMetadataIntoImmutableSnapshot() {
        Node broker1 = new Node(1, "broker-1", 9092);
        Node broker2 = new Node(2, "broker-2", 9092);
        Node broker3 = new Node(3, "broker-3", 9092);
        TopicPartitionInfo partition = new TopicPartitionInfo(
                0, broker2, List.of(broker1, broker2, broker3), List.of(broker2, broker3));
        TopicDescription topic = new TopicDescription("orders", false, List.of(partition));

        ClusterMetadataSnapshot snapshot = KafkaMetadataSnapshotCollector.toSnapshot(
                "cluster-1",
                observedAt,
                List.of(broker1, broker2, broker3),
                Map.of("orders", topic));

        assertEquals("cluster-1", snapshot.clusterId());
        assertEquals(observedAt, snapshot.observedAt());
        assertEquals(Set.of(1, 2, 3), snapshot.brokerIds());

        PartitionMetadataSnapshot metadata =
                snapshot.partitions().get(new PartitionRef("orders", 0));
        assertNotNull(metadata);
        assertEquals(2, metadata.leaderId());
        assertEquals(Set.of(1, 2, 3), metadata.replicaIds());
        assertEquals(Set.of(2, 3), metadata.isrIds());
    }

    @Test
    void preservesPartitionWithoutLeader() {
        Node broker1 = new Node(1, "broker-1", 9092);
        Node broker2 = new Node(2, "broker-2", 9092);
        TopicPartitionInfo partition = new TopicPartitionInfo(
                0, null, List.of(broker1, broker2), List.of(broker1));
        TopicDescription topic = new TopicDescription("orders", false, List.of(partition));

        ClusterMetadataSnapshot snapshot = KafkaMetadataSnapshotCollector.toSnapshot(
                "cluster-1", observedAt, List.of(broker1, broker2), Map.of("orders", topic));

        assertNull(snapshot.partitions().get(new PartitionRef("orders", 0)).leaderId());
    }

    @Test
    void permitsUnavailableReplicaNotPresentInBrokerList() {
        Node broker1 = new Node(1, "broker-1", 9092);
        Node broker2 = new Node(2, "broker-2", 9092);
        Node unavailableBroker = new Node(3, "broker-3", 9092);
        TopicPartitionInfo partition = new TopicPartitionInfo(
                0, broker1, List.of(broker1, broker2, unavailableBroker), List.of(broker1, broker2));
        TopicDescription topic = new TopicDescription("orders", false, List.of(partition));

        ClusterMetadataSnapshot snapshot = KafkaMetadataSnapshotCollector.toSnapshot(
                "cluster-1", observedAt, List.of(broker1, broker2), Map.of("orders", topic));

        assertEquals(Set.of(1, 2), snapshot.brokerIds());
        assertEquals(Set.of(1, 2, 3),
                snapshot.partitions().get(new PartitionRef("orders", 0)).replicaIds());
    }

    @Test
    void rejectsMissingClientProperties() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaMetadataSnapshotCollector().collect(null, 10_000));
    }

    @Test
    void rejectsTimeoutOutsideBoundsBeforeConnecting() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        assertThrows(IllegalArgumentException.class,
                () -> new KafkaMetadataSnapshotCollector().collect(properties, 99));
    }
}
