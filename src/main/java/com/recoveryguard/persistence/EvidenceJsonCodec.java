package com.recoveryguard.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.PartitionMetadataSnapshot;
import com.recoveryguard.evidence.PartitionRef;
import com.recoveryguard.evidence.RecoveryEvent;
import com.recoveryguard.evidence.RecoveryTimelineEntry;
import com.recoveryguard.evidence.RecoverySignal;
import com.recoveryguard.evidence.TimelineEntryType;
import com.recoveryguard.evidence.TimelineOrderingConfidence;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
        EventPayload event = entry.event() == null ? null : EventPayload.from(entry.event());
        return write(new TimelinePayload(
                entry.sequence(), entry.observationIndex(), entry.observedAt(),
                entry.occurredAt(), entry.type().name(), entry.orderingConfidence().name(), event));
    }

    ClusterMetadataSnapshot decodeSnapshot(String json) {
        SnapshotPayload payload = read(json, SnapshotPayload.class);
        Map<PartitionRef, PartitionMetadataSnapshot> partitions = payload.partitions().stream()
                .collect(Collectors.toMap(
                        item -> new PartitionRef(item.topic(), item.partition()),
                        item -> new PartitionMetadataSnapshot(
                                item.leaderId(), Set.copyOf(item.replicaIds()), Set.copyOf(item.isrIds()))));
        return new ClusterMetadataSnapshot(payload.clusterId(), payload.observedAt(),
                Set.copyOf(payload.brokerIds()), partitions);
    }

    RecoveryTimelineEntry decodeTimelineEntry(String json) {
        TimelinePayload payload = read(json, TimelinePayload.class);
        RecoveryEvent event = payload.event() == null ? null : new RecoveryEvent(
                payload.event().eventId(), payload.event().clusterId(), payload.event().observedAt(),
                RecoverySignal.valueOf(payload.event().signal()),
                Set.copyOf(payload.event().affectedPartitions()),
                Set.copyOf(payload.event().affectedBrokerIds()));
        ClusterMetadataSnapshot snapshot = null;
        if (payload.type().equals(TimelineEntryType.SNAPSHOT.name())) {
            throw new PersistenceException(
                    "Snapshot timeline entries must be joined with their stored observation");
        }
        return new RecoveryTimelineEntry(
                payload.sequence(), payload.observationIndex(), payload.observedAt(),
                payload.occurredAt(), TimelineEntryType.valueOf(payload.type()),
                TimelineOrderingConfidence.valueOf(payload.orderingConfidence()), snapshot, event);
    }

    RecoveryTimelineEntry decodeSnapshotTimelineEntry(String json, ClusterMetadataSnapshot snapshot) {
        TimelinePayload payload = read(json, TimelinePayload.class);
        return new RecoveryTimelineEntry(
                payload.sequence(), payload.observationIndex(), payload.observedAt(),
                payload.occurredAt(), TimelineEntryType.valueOf(payload.type()),
                TimelineOrderingConfidence.valueOf(payload.orderingConfidence()), snapshot, null);
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
            EventPayload event
    ) {}
}
