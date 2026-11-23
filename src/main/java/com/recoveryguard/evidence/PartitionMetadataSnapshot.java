package com.recoveryguard.evidence;

import java.util.Set;

/**
 * Read-only metadata for one Kafka partition at one observation time.
 * A null leaderId means that Kafka reported no current leader.
 */
public record PartitionMetadataSnapshot(
        Integer leaderId,
        Set<Integer> replicaIds,
        Set<Integer> isrIds
) {
    public PartitionMetadataSnapshot {
        if (leaderId != null && leaderId < 0) {
            throw new IllegalArgumentException("leaderId cannot be negative");
        }
        if (replicaIds == null) {
            throw new IllegalArgumentException("replicaIds is required");
        }
        if (isrIds == null) {
            throw new IllegalArgumentException("isrIds is required");
        }

        replicaIds = Set.copyOf(replicaIds);
        isrIds = Set.copyOf(isrIds);

        if (replicaIds.stream().anyMatch(id -> id == null || id < 0)) {
            throw new IllegalArgumentException("replicaIds must contain non-negative broker IDs");
        }
        if (isrIds.stream().anyMatch(id -> id == null || id < 0)) {
            throw new IllegalArgumentException("isrIds must contain non-negative broker IDs");
        }
        if (!replicaIds.containsAll(isrIds)) {
            throw new IllegalArgumentException("isrIds must be a subset of replicaIds");
        }
        if (leaderId != null && !replicaIds.contains(leaderId)) {
            throw new IllegalArgumentException("leaderId must be a replica");
        }
    }
}
