package com.recoveryguard.evidence;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Compares two immutable Kafka metadata snapshots and emits observed changes.
 *
 * <p>This is a pure comparison component. It does not connect to Kafka,
 * read records, persist evidence, or decide whether a recovery was correct.</p>
 */
public final class RecoveryChangeDetector {

    public List<RecoveryEvent> detect(
            ClusterMetadataSnapshot previous,
            ClusterMetadataSnapshot current) {

        Objects.requireNonNull(previous, "previous snapshot is required");
        Objects.requireNonNull(current, "current snapshot is required");

        if (!previous.clusterId().equals(current.clusterId())) {
            throw new IllegalArgumentException("snapshots must belong to the same cluster");
        }
        if (current.observedAt().isBefore(previous.observedAt())) {
            throw new IllegalArgumentException("current snapshot cannot be older than previous snapshot");
        }

        List<RecoveryEvent> events = new ArrayList<>();
        detectBrokerChanges(previous, current, events);
        detectPartitionChanges(previous, current, events);
        return List.copyOf(events);
    }

    private static void detectBrokerChanges(
            ClusterMetadataSnapshot previous,
            ClusterMetadataSnapshot current,
            List<RecoveryEvent> events) {

        Set<Integer> unavailable = new HashSet<>(previous.brokerIds());
        unavailable.removeAll(current.brokerIds());

        Set<Integer> available = new HashSet<>(current.brokerIds());
        available.removeAll(previous.brokerIds());

        unavailable.stream().sorted().forEach(brokerId ->
                events.add(event(current, RecoverySignal.BROKER_UNAVAILABLE,
                        partitionsAssignedTo(previous, current, brokerId), Set.of(brokerId))));

        available.stream().sorted().forEach(brokerId ->
                events.add(event(current, RecoverySignal.BROKER_AVAILABLE,
                        partitionsAssignedTo(previous, current, brokerId), Set.of(brokerId))));
    }

    private static void detectPartitionChanges(
            ClusterMetadataSnapshot previous,
            ClusterMetadataSnapshot current,
            List<RecoveryEvent> events) {

        List<PartitionRef> commonPartitions = previous.partitions().keySet().stream()
                .filter(current.partitions()::containsKey)
                .sorted(Comparator.comparing(PartitionRef::topic)
                        .thenComparingInt(PartitionRef::partition))
                .toList();

        for (PartitionRef partition : commonPartitions) {
            PartitionMetadataSnapshot before = previous.partitions().get(partition);
            PartitionMetadataSnapshot after = current.partitions().get(partition);

            if (!Objects.equals(before.leaderId(), after.leaderId())) {
                events.add(event(current, RecoverySignal.LEADER_CHANGED,
                        Set.of(partition), changedLeaderIds(before.leaderId(), after.leaderId())));
            }

            if (!before.isrIds().equals(after.isrIds())) {
                events.add(event(current, RecoverySignal.ISR_CHANGED,
                        Set.of(partition), symmetricDifference(before.isrIds(), after.isrIds())));
            }
        }
    }

    private static Set<PartitionRef> partitionsAssignedTo(
            ClusterMetadataSnapshot previous,
            ClusterMetadataSnapshot current,
            int brokerId) {

        Set<PartitionRef> affected = new LinkedHashSet<>();
        collectAssignedPartitions(previous, brokerId, affected);
        collectAssignedPartitions(current, brokerId, affected);
        return Set.copyOf(affected);
    }

    private static void collectAssignedPartitions(
            ClusterMetadataSnapshot snapshot,
            int brokerId,
            Set<PartitionRef> affected) {

        snapshot.partitions().forEach((partition, metadata) -> {
            if (metadata.replicaIds().contains(brokerId)) {
                affected.add(partition);
            }
        });
    }

    private static Set<Integer> changedLeaderIds(Integer before, Integer after) {
        Set<Integer> changed = new LinkedHashSet<>();
        if (before != null) {
            changed.add(before);
        }
        if (after != null) {
            changed.add(after);
        }
        return Set.copyOf(changed);
    }

    private static Set<Integer> symmetricDifference(Set<Integer> before, Set<Integer> after) {
        Set<Integer> changed = new LinkedHashSet<>(before);
        for (Integer brokerId : after) {
            if (!changed.add(brokerId)) {
                changed.remove(brokerId);
            }
        }
        return Set.copyOf(changed);
    }

    private static RecoveryEvent event(
            ClusterMetadataSnapshot snapshot,
            RecoverySignal signal,
            Set<PartitionRef> partitions,
            Set<Integer> brokers) {

        return new RecoveryEvent(UUID.randomUUID(), snapshot.clusterId(),
                snapshot.observedAt(), signal, partitions, brokers);
    }
}
