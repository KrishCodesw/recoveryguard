package com.recoveryguard.evidence;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Immutable cluster metadata collected at one point in time.
 *
 * <p>Partition replica assignments can contain broker IDs that are absent
 * from brokerIds. This represents unavailable replicas.</p>
 */
public record ClusterMetadataSnapshot(
        String clusterId,
        Instant observedAt,
        Set<Integer> brokerIds,
        Map<PartitionRef, PartitionMetadataSnapshot> partitions
) {
    public ClusterMetadataSnapshot {
        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId cannot be blank");
        }
        if (observedAt == null) {
            throw new IllegalArgumentException("observedAt is required");
        }
        if (brokerIds == null) {
            throw new IllegalArgumentException("brokerIds is required");
        }
        if (partitions == null) {
            throw new IllegalArgumentException("partitions is required");
        }

        brokerIds = Set.copyOf(brokerIds);
        partitions = Map.copyOf(partitions);

        if (brokerIds.stream().anyMatch(id -> id == null || id < 0)) {
            throw new IllegalArgumentException("brokerIds must contain non-negative broker IDs");
        }
        if (partitions.entrySet().stream()
                .anyMatch(entry -> entry.getKey() == null || entry.getValue() == null)) {
            throw new IllegalArgumentException("partitions cannot contain null keys or values");
        }
    }
}
