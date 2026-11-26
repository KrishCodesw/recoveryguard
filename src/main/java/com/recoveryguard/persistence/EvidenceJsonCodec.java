package com.recoveryguard.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.PartitionMetadataSnapshot;
import com.recoveryguard.evidence.PartitionRef;
import com.recoveryguard.evidence.RecoveryEvent;
import com.recoveryguard.evidence.RecoveryTimelineEntry;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Produces stable JSON payloads for evidence hashing and storage. */
final class EvidenceJsonCodec {
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .findAndRegisterModules();

    String encodeSnapshot(ClusterMetadataSnapshot snapshot) {
        List<PartitionPayload> partitions = snapshot.partitions().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(PartitionRef::topic)
                                .thenComparingInt(PartitionRef::partition)))
                .map(entry -> new PartitionPayload(
                        entry.getKey().topic(),
                        entry.getKey().partition(),
                        entry.getValue().leaderId(),
                        entry.getValue().replicaIds().stream().sorted().toList(),
                        entry.getValue().isrIds().stream().sorted().toList()))
                .toList();
        return write(new SnapshotPayload(
                snapshot.clusterId(), snapshot.observedAt(),
                snapshot.brokerIds().stream().sorted().toList(), partitions));
    }

    String encodeTimelineEntry(RecoveryTimelineEntry entry) {
        SnapshotPayload snapshot = null;
        if (entry.snapshot() != null) {
            String json = encodeSnapshot(entry.snapshot());
            snapshot = read(json, SnapshotPayload.class);
        }
        EventPayload event = entry.event() == null ? null : EventPayload.from(entry.event());
        return write(new TimelinePayload(
                entry.sequence(), entry.observationIndex(), entry.observedAt(),
                entry.occurredAt(), entry.type().name(), entry.orderingConfidence().name(),
                snapshot, event));
    }

    private String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new PersistenceException("Could not serialize evidence", exception);
        }
    }

    private <T> T read(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException exception) {
            throw new PersistenceException("Could not prepare evidence payload", exception);
        }
    }

    record SnapshotPayload(
            String clusterId,
            Instant observedAt,
            List<Integer> brokerIds,
            List<PartitionPayload> partitions
    ) {}

    record PartitionPayload(
            String topic,
            int partition,
            Integer leaderId,
            List<Integer> replicaIds,
            List<Integer> isrIds
    ) {}

    record EventPayload(
            UUID eventId,
            String clusterId,
            Instant observedAt,
            String signal,
            List<PartitionRef> affectedPartitions,
            List<Integer> affectedBrokerIds
    ) {
        static EventPayload from(RecoveryEvent event) {
            return new EventPayload(
                    event.eventId(), event.clusterId(), event.observedAt(),
                    event.signal().name(),
                    event.affectedPartitions().stream()
                            .sorted(Comparator.comparing(PartitionRef::topic)
                                    .thenComparingInt(PartitionRef::partition))
                            .toList(),
                    event.affectedBrokerIds().stream().sorted().toList());
        }
    }

    record TimelinePayload(
            long sequence,
            int observationIndex,
            Instant observedAt,
            Instant occurredAt,
            String type,
            String orderingConfidence,
            SnapshotPayload snapshot,
            EventPayload event
    ) {}
}
