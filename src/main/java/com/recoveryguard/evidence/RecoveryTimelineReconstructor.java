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
    private static final Comparator<PartitionRef> PARTITION_ORDER =
            Comparator.comparing(PartitionRef::topic)
                    .thenComparingInt(PartitionRef::partition);

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
        if (snapshots.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("snapshots cannot contain null");
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

            List<RecoveryEvent> orderedEvents = changeDetector.detect(input.get(index - 1), current)
                    .stream()
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
        return (left, right) -> {
            int signalOrder = Integer.compare(signalPriority(left.signal()), signalPriority(right.signal()));
            if (signalOrder != 0) {
                return signalOrder;
            }

            int partitionOrder = compareLists(sortedPartitions(left), sortedPartitions(right),
                    PARTITION_ORDER);
            if (partitionOrder != 0) {
                return partitionOrder;
            }

            return compareLists(sortedBrokers(left), sortedBrokers(right), Integer::compareTo);
        };
    }

    private static int signalPriority(RecoverySignal signal) {
        return switch (signal) {
            case BROKER_UNAVAILABLE -> 0;
            case BROKER_AVAILABLE -> 1;
            case LEADER_CHANGED -> 2;
            case ISR_CHANGED -> 3;
        };
    }

    private static List<PartitionRef> sortedPartitions(RecoveryEvent event) {
        return event.affectedPartitions().stream().sorted(PARTITION_ORDER).toList();
    }

    private static List<Integer> sortedBrokers(RecoveryEvent event) {
        return event.affectedBrokerIds().stream().sorted().toList();
    }

    private static <T> int compareLists(List<T> left, List<T> right, Comparator<T> comparator) {
        int commonSize = Math.min(left.size(), right.size());
        for (int index = 0; index < commonSize; index++) {
            int result = comparator.compare(left.get(index), right.get(index));
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(left.size(), right.size());
    }

    private static boolean isBrokerEvent(RecoverySignal signal) {
        return signal == RecoverySignal.BROKER_UNAVAILABLE
                || signal == RecoverySignal.BROKER_AVAILABLE;
    }
}
