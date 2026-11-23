package com.recoveryguard.evidence;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reconstructs a timeline from ordered metadata snapshots and detected changes.
 *
 * <p>The input order is the collection order. Timestamps must not move
 * backwards. Equal timestamps are retained in input order but marked as
 * temporally uncertain. No causal relationship is inferred.</p>
 */
public final class RecoveryTimelineReconstructor {
    private final RecoveryChangeDetector changeDetector;

    public RecoveryTimelineReconstructor() {
        this(new RecoveryChangeDetector());
    }

    RecoveryTimelineReconstructor(RecoveryChangeDetector changeDetector) {
        this.changeDetector = Objects.requireNonNull(changeDetector, "changeDetector is required");
    }

    public RecoveryTimeline reconstruct(List<ClusterMetadataSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            throw new IllegalArgumentException("at least one snapshot is required");
        }

        List<ClusterMetadataSnapshot> input = List.copyOf(snapshots);
        String clusterId = input.get(0).clusterId();
        validateSnapshots(input, clusterId);

        Map<Instant, Integer> timestampCounts = new HashMap<>();
        input.forEach(snapshot -> timestampCounts.merge(snapshot.observedAt(), 1, Integer::sum));

        List<RecoveryTimelineEntry> entries = new ArrayList<>();
        long sequence = 1;

        for (int index = 0; index < input.size(); index++) {
            ClusterMetadataSnapshot current = input.get(index);
            boolean timestampRepeated = timestampCounts.get(current.observedAt()) > 1;

            TimelineOrderingConfidence snapshotConfidence = timestampRepeated
                    ? TimelineOrderingConfidence.UNKNOWN_ORDER
                    : TimelineOrderingConfidence.OBSERVED_ORDER;

            entries.add(new RecoveryTimelineEntry(
                    sequence++, index, current.observedAt(), null,
                    TimelineEntryType.SNAPSHOT, snapshotConfidence, current, null));

            if (index == 0) {
                continue;
            }

            List<RecoveryEvent> events = changeDetector.detect(input.get(index - 1), current);
            List<RecoveryEvent> orderedEvents = events.stream()
                    .sorted(eventComparator())
                    .toList();

            for (RecoveryEvent event : orderedEvents) {
                TimelineEntryType type = isBrokerEvent(event.signal())
                        ? TimelineEntryType.BROKER_EVENT
                        : TimelineEntryType.PARTITION_EVENT;
                TimelineOrderingConfidence confidence = timestampRepeated
                        ? TimelineOrderingConfidence.UNKNOWN_ORDER
                        : TimelineOrderingConfidence.SAME_OBSERVATION;

                entries.add(new RecoveryTimelineEntry(
                        sequence++, index, current.observedAt(), null,
                        type, confidence, null, event));
            }
        }

        return new RecoveryTimeline(clusterId, entries);
    }

    private static void validateSnapshots(List<ClusterMetadataSnapshot> snapshots, String clusterId) {
        Instant previousTime = null;
        for (ClusterMetadataSnapshot snapshot : snapshots) {
            if (snapshot == null) {
                throw new IllegalArgumentException("snapshots cannot contain null");
            }
            if (!clusterId.equals(snapshot.clusterId())) {
                throw new IllegalArgumentException("snapshots must belong to the same cluster");
            }
            if (previousTime != null && snapshot.observedAt().isBefore(previousTime)) {
                throw new IllegalArgumentException("snapshots must be in non-decreasing observation order");
            }
            previousTime = snapshot.observedAt();
        }
    }

    private static Comparator<RecoveryEvent> eventComparator() {
        return Comparator.comparingInt((RecoveryEvent event) -> event.signal().ordinal())
                .thenComparing(RecoveryTimelineReconstructor::partitionSortKey)
                .thenComparing(RecoveryTimelineReconstructor::brokerSortKey);
    }

    private static String partitionSortKey(RecoveryEvent event) {
        return event.affectedPartitions().stream()
                .sorted(Comparator.comparing(PartitionRef::topic)
                        .thenComparingInt(PartitionRef::partition))
                .map(partition -> partition.topic() + "\u0000" + partition.partition())
                .reduce((left, right) -> left + "\u0001" + right)
                .orElse("");
    }

    private static String brokerSortKey(RecoveryEvent event) {
        return event.affectedBrokerIds().stream()
                .sorted()
                .map(String::valueOf)
                .reduce((left, right) -> left + "," + right)
                .orElse("");
    }

    private static boolean isBrokerEvent(RecoverySignal signal) {
        return signal == RecoverySignal.BROKER_UNAVAILABLE
                || signal == RecoverySignal.BROKER_AVAILABLE;
    }
}
