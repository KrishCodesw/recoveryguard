package com.recoveryguard.evidence;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record RecoveryEvent(
        UUID eventId,
        String clusterId,
        Instant observedAt,
        RecoverySignal signal,
        Set<PartitionRef> affectedPartitions,
        Set<Integer> affectedBrokerIds
) {

    /**
     * Compatibility constructor for events that do not identify broker IDs.
     */
    public RecoveryEvent(
            UUID eventId,
            String clusterId,
            Instant observedAt,
            RecoverySignal signal,
            Set<PartitionRef> affectedPartitions) {
        this(eventId, clusterId, observedAt, signal, affectedPartitions, Set.of());
    }

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
        if (affectedBrokerIds == null) {
            throw new IllegalArgumentException("affectedBrokerIds is required");
        }

        affectedPartitions = Set.copyOf(affectedPartitions);
        affectedBrokerIds = Set.copyOf(affectedBrokerIds);

        if (affectedBrokerIds.stream().anyMatch(id -> id == null || id < 0)) {
            throw new IllegalArgumentException(
                    "affectedBrokerIds must contain non-negative broker IDs");
        }
    }
}
