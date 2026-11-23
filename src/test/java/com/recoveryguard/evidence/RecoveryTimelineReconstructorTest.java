package com.recoveryguard.evidence;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryTimelineReconstructorTest {
    private final RecoveryTimelineReconstructor reconstructor = new RecoveryTimelineReconstructor();
    private final Instant t1 = Instant.parse("2026-01-01T10:00:00Z");
    private final Instant t2 = Instant.parse("2026-01-01T10:00:10Z");
    private final PartitionRef orders0 = new PartitionRef("orders", 0);

    @Test
    void retainsEverySnapshotIncludingSnapshotsWithoutChanges() {
        var first = snapshot(t1, Set.of(1, 2), partition(1, Set.of(1, 2), Set.of(1, 2)));
        var second = snapshot(t2, Set.of(1, 2), partition(1, Set.of(1, 2), Set.of(1, 2)));

        RecoveryTimeline timeline = reconstructor.reconstruct(List.of(first, second));

        assertEquals(2, timeline.entries().size());
        assertEquals(TimelineEntryType.SNAPSHOT, timeline.entries().get(0).type());
        assertEquals(TimelineEntryType.SNAPSHOT, timeline.entries().get(1).type());
        assertEquals(first, timeline.entries().get(0).snapshot());
        assertEquals(second, timeline.entries().get(1).snapshot());
        assertEquals(List.of(1L, 2L), timeline.entries().stream()
                .map(RecoveryTimelineEntry::sequence).toList());
    }

    @Test
    void reconstructsBrokerAndPartitionEventsInStableOrder() {
        var first = snapshot(t1, Set.of(1, 2),
                partition(1, Set.of(1, 2), Set.of(1, 2)));
        var second = snapshot(t2, Set.of(1),
                partition(1, Set.of(1, 2), Set.of(1)));

        RecoveryTimeline timeline = reconstructor.reconstruct(List.of(first, second));

        assertEquals(4, timeline.entries().size());
        assertEquals(TimelineEntryType.SNAPSHOT, timeline.entries().get(0).type());
        assertEquals(TimelineEntryType.SNAPSHOT, timeline.entries().get(1).type());
        assertEquals(TimelineEntryType.BROKER_EVENT, timeline.entries().get(2).type());
        assertEquals(RecoverySignal.BROKER_UNAVAILABLE, timeline.entries().get(2).event().signal());
        assertEquals(TimelineEntryType.PARTITION_EVENT, timeline.entries().get(3).type());
        assertEquals(RecoverySignal.ISR_CHANGED, timeline.entries().get(3).event().signal());
        assertEquals(TimelineOrderingConfidence.SAME_OBSERVATION,
                timeline.entries().get(2).orderingConfidence());
        assertTrue(timeline.entries().stream().allMatch(entry -> entry.occurredAt() == null));
    }

    @Test
    void ordersLeaderAndIsrEventsByStableSignalPriority() {
        var first = snapshot(t1, Set.of(1, 2, 3),
                partition(1, Set.of(1, 2, 3), Set.of(1, 2)));
        var second = snapshot(t2, Set.of(1, 2, 3),
                partition(2, Set.of(1, 2, 3), Set.of(1, 3)));

        RecoveryTimeline timeline = reconstructor.reconstruct(List.of(first, second));

        assertEquals(RecoverySignal.LEADER_CHANGED, timeline.entries().get(2).event().signal());
        assertEquals(RecoverySignal.ISR_CHANGED, timeline.entries().get(3).event().signal());
    }

    @Test
    void marksEventsUnknownWhenObservationsShareTimestamp() {
        var first = snapshot(t1, Set.of(1, 2),
                partition(1, Set.of(1, 2), Set.of(1, 2)));
        var second = snapshot(t1, Set.of(1),
                partition(1, Set.of(1, 2), Set.of(1)));

        RecoveryTimeline timeline = reconstructor.reconstruct(List.of(first, second));

        assertTrue(timeline.entries().stream().allMatch(entry ->
                entry.orderingConfidence() == TimelineOrderingConfidence.UNKNOWN_ORDER));
    }

    @Test
    void rejectsEmptyInput() {
        assertThrows(IllegalArgumentException.class, () -> reconstructor.reconstruct(List.of()));
    }

    @Test
    void rejectsSnapshotsFromDifferentClusters() {
        var first = snapshot("cluster-a", t1, Set.of(1), Map.of());
        var second = snapshot("cluster-b", t2, Set.of(1), Map.of());

        assertThrows(IllegalArgumentException.class,
                () -> reconstructor.reconstruct(List.of(first, second)));
    }

    @Test
    void rejectsSnapshotsThatMoveBackwardsInTime() {
        var first = snapshot(t2, Set.of(1), Map.of());
        var second = snapshot(t1, Set.of(1), Map.of());

        assertThrows(IllegalArgumentException.class,
                () -> reconstructor.reconstruct(List.of(first, second)));
    }

    private ClusterMetadataSnapshot snapshot(
            Instant time, Set<Integer> brokers, PartitionMetadataSnapshot partition) {
        return snapshot("cluster-1", time, brokers, Map.of(orders0, partition));
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
