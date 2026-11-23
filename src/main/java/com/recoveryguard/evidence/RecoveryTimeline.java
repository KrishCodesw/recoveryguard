package com.recoveryguard.evidence;

import java.util.List;

/** Immutable, ordered evidence timeline for one Kafka cluster. */
public record RecoveryTimeline(
        String clusterId,
        List<RecoveryTimelineEntry> entries
) {
    public RecoveryTimeline {
        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId cannot be blank");
        }
        if (entries == null) {
            throw new IllegalArgumentException("entries is required");
        }
        entries = List.copyOf(entries);

        for (int index = 0; index < entries.size(); index++) {
            RecoveryTimelineEntry entry = entries.get(index);
            if (entry == null) {
                throw new IllegalArgumentException("entries cannot contain null");
            }
            if (entry.sequence() != index + 1L) {
                throw new IllegalArgumentException("timeline sequence must start at 1 and be contiguous");
            }
            if (entry.snapshot() != null && !clusterId.equals(entry.snapshot().clusterId())) {
                throw new IllegalArgumentException("snapshot belongs to a different cluster");
            }
            if (entry.event() != null && !clusterId.equals(entry.event().clusterId())) {
                throw new IllegalArgumentException("event belongs to a different cluster");
            }
        }
    }
}
