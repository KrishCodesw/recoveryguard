package com.recoveryguard.evidence;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record RecoveryEvent(
        UUID eventId,
        String clusterId,
        Instant observedAt,
        RecoverySignal signal,
        Set<PartitionRef> affectedPartitions
) {

    public RecoveryEvent {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId is required");
        }

        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId cannot be blank");
        }

        if (observedAt == null) {
            throw new IllegalArgumentException("observedAt is required");
        }

        if (signal == null) {
            throw new IllegalArgumentException("signal is required");
        }

        if (affectedPartitions == null) {
            throw new IllegalArgumentException("affectedPartitions is required");
        }

        affectedPartitions = Set.copyOf(affectedPartitions);
    }
}
