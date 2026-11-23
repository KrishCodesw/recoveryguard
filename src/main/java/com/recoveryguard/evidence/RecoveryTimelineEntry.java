package com.recoveryguard.evidence;

import java.time.Instant;

/**
 * One ordered item in a recovery timeline.
 *
 * <p>A snapshot entry retains the complete immutable snapshot. An event entry
 * retains the event detected from that observation. occurredAt is null unless
 * a trusted source provides the actual Kafka transition time.</p>
 */
public record RecoveryTimelineEntry(
        long sequence,
        int observationIndex,
        Instant observedAt,
        Instant occurredAt,
        TimelineEntryType type,
        TimelineOrderingConfidence orderingConfidence,
        ClusterMetadataSnapshot snapshot,
        RecoveryEvent event
) {
    public RecoveryTimelineEntry {
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        if (observationIndex < 0) {
            throw new IllegalArgumentException("observationIndex cannot be negative");
        }
        if (observedAt == null) {
            throw new IllegalArgumentException("observedAt is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }
        if (orderingConfidence == null) {
            throw new IllegalArgumentException("orderingConfidence is required");
        }

        boolean hasSnapshot = snapshot != null;
        boolean hasEvent = event != null;
        if (hasSnapshot == hasEvent) {
            throw new IllegalArgumentException("entry must contain exactly one snapshot or event");
        }
        if (type == TimelineEntryType.SNAPSHOT && !hasSnapshot) {
            throw new IllegalArgumentException("SNAPSHOT entry must contain a snapshot");
        }
        if (type != TimelineEntryType.SNAPSHOT && !hasEvent) {
            throw new IllegalArgumentException("event entry must contain an event");
        }
        if (hasSnapshot && !snapshot.observedAt().equals(observedAt)) {
            throw new IllegalArgumentException("snapshot timestamp must match observedAt");
        }
        if (hasEvent && (!event.observedAt().equals(observedAt))) {
            throw new IllegalArgumentException("event timestamp must match observedAt");
        }
    }
}
