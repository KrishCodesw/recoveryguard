package com.recoveryguard.evidence;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryChangeDetectorTest {

    private final RecoveryChangeDetector detector = new RecoveryChangeDetector();
    private final Instant beforeTime = Instant.parse("2026-01-01T10:00:00Z");
    private final Instant afterTime = Instant.parse("2026-01-01T10:00:10Z");
    private final PartitionRef orders0 = new PartitionRef("orders", 0);

    @Test
    void detectsBrokerUnavailableAndAvailable() {
        ClusterMetadataSnapshot before = snapshot(beforeTime, Set.of(1, 2),
                Map.of(orders0, partition(1, Set.of(1, 2), Set.of(1))));
        ClusterMetadataSnapshot after = snapshot(afterTime, Set.of(1, 3),
                Map.of(orders0, partition(1, Set.of(1, 2), Set.of(1))));

        var events = detector.detect(before, after);

        assertEquals(2, events.size());
        assertEquals(RecoverySignal.BROKER_UNAVAILABLE, events.get(0).signal());
        assertEquals(Set.of(2), events.get(0).affectedBrokerIds());
        assertEquals(Set.of(orders0), events.get(0).affectedPartitions());
        assertEquals(RecoverySignal.BROKER_AVAILABLE, events.get(1).signal());
        assertEquals(Set.of(3), events.get(1).affectedBrokerIds());
    }

    @Test
    void detectsLeaderChangeAndNamesOldAndNewLeaders() {
        ClusterMetadataSnapshot before = snapshot(beforeTime, Set.of(1, 2),
                Map.of(orders0, partition(1, Set.of(1, 2), Set.of(1, 2))));
        ClusterMetadataSnapshot after = snapshot(afterTime, Set.of(1, 2),
                Map.of(orders0, partition(2, Set.of(1, 2), Set.of(1, 2))));

        var events = detector.detect(before, after);

        assertEquals(1, events.size());
        assertEquals(RecoverySignal.LEADER_CHANGED, events.get(0).signal());
        assertEquals(Set.of(1, 2), events.get(0).affectedBrokerIds());
        assertEquals(Set.of(orders0), events.get(0).affectedPartitions());
    }

    @Test
    void detectsIsrMembershipChange() {
        ClusterMetadataSnapshot before = snapshot(beforeTime, Set.of(1, 2, 3),
                Map.of(orders0, partition(1, Set.of(1, 2, 3), Set.of(1, 2))));
        ClusterMetadataSnapshot after = snapshot(afterTime, Set.of(1, 2, 3),
                Map.of(orders0, partition(1, Set.of(1, 2, 3), Set.of(1, 3))));

        var events = detector.detect(before, after);

        assertEquals(1, events.size());
        assertEquals(RecoverySignal.ISR_CHANGED, events.get(0).signal());
        assertEquals(Set.of(2, 3), events.get(0).affectedBrokerIds());
    }

    @Test
    void returnsNoEventsWhenMetadataIsUnchanged() {
        var metadata = Map.of(orders0, partition(1, Set.of(1, 2), Set.of(1, 2)));
        ClusterMetadataSnapshot before = snapshot(beforeTime, Set.of(1, 2), metadata);
        ClusterMetadataSnapshot after = snapshot(afterTime, Set.of(1, 2), metadata);

        assertTrue(detector.detect(before, after).isEmpty());
    }

    @Test
    void doesNotTreatNewOrDeletedPartitionsAsStateTransitions() {
        PartitionRef newPartition = new PartitionRef("orders", 1);
        ClusterMetadataSnapshot before = snapshot(beforeTime, Set.of(1, 2),
                Map.of(orders0, partition(1, Set.of(1, 2), Set.of(1, 2))));
        ClusterMetadataSnapshot after = snapshot(afterTime, Set.of(1, 2),
                Map.of(newPartition, partition(2, Set.of(1, 2), Set.of(1, 2))));

        assertTrue(detector.detect(before, after).isEmpty());
    }

    @Test
    void rejectsSnapshotsFromDifferentClusters() {
        ClusterMetadataSnapshot before = snapshot("cluster-a", beforeTime, Set.of(1), Map.of());
        ClusterMetadataSnapshot after = snapshot("cluster-b", afterTime, Set.of(1), Map.of());

        assertThrows(IllegalArgumentException.class, () -> detector.detect(before, after));
    }

    @Test
    void rejectsOlderCurrentSnapshot() {
        ClusterMetadataSnapshot before = snapshot(afterTime, Set.of(1), Map.of());
        ClusterMetadataSnapshot after = snapshot(beforeTime, Set.of(1), Map.of());

        assertThrows(IllegalArgumentException.class, () -> detector.detect(before, after));
    }

    @Test
    void detectsTransitionFromLeaderToNoLeader() {
        ClusterMetadataSnapshot before = snapshot(beforeTime, Set.of(1, 2),
                Map.of(orders0, partition(1, Set.of(1, 2), Set.of(1, 2))));
        ClusterMetadataSnapshot after = snapshot(afterTime, Set.of(1, 2),
                Map.of(orders0, partition(null, Set.of(1, 2), Set.of(1, 2))));

        var events = detector.detect(before, after);

        assertEquals(1, events.size());
        assertEquals(RecoverySignal.LEADER_CHANGED, events.get(0).signal());
        assertEquals(Set.of(1), events.get(0).affectedBrokerIds());
    }

    private static ClusterMetadataSnapshot snapshot(
            Instant time, Set<Integer> brokers,
            Map<PartitionRef, PartitionMetadataSnapshot> partitions) {
        return snapshot("cluster-1", time, brokers, partitions);
    }

    private static ClusterMetadataSnapshot snapshot(
            String clusterId, Instant time, Set<Integer> brokers,
            Map<PartitionRef, PartitionMetadataSnapshot> partitions) {
        return new ClusterMetadataSnapshot(clusterId, time, brokers, partitions);
    }

    private static PartitionMetadataSnapshot partition(
            Integer leader, Set<Integer> replicas, Set<Integer> isr) {
        return new PartitionMetadataSnapshot(leader, replicas, isr);
    }
}
