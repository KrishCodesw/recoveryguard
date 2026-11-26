package com.recoveryguard.persistence;

import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.PartitionMetadataSnapshot;
import com.recoveryguard.evidence.PartitionRef;
import com.recoveryguard.evidence.RecoveryTimeline;
import com.recoveryguard.evidence.RecoveryTimelineReconstructor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SQLiteEvidenceStoreTest {
    @TempDir
    Path tempDir;

    private final Instant firstTime = Instant.parse("2026-11-27T00:00:00Z");
    private final Instant secondTime = Instant.parse("2026-11-27T00:00:10Z");

    @Test
    void storesSnapshotsAndTimelineAndCompletesInvestigation() {
        SQLiteEvidenceStore store = newStore();
        store.initialize();

        ClusterMetadataSnapshot first = snapshot(firstTime, Set.of(1, 2),
                new PartitionMetadataSnapshot(1, Set.of(1, 2), Set.of(1, 2)));
        ClusterMetadataSnapshot second = snapshot(secondTime, Set.of(1, 2),
                new PartitionMetadataSnapshot(2, Set.of(1, 2), Set.of(1, 2)));

        UUID investigationId = store.createInvestigation("cluster-1", firstTime);
        store.appendObservation(investigationId, 0, first);
        store.appendObservation(investigationId, 1, second);

        RecoveryTimeline timeline = new RecoveryTimelineReconstructor()
                .reconstruct(List.of(first, second));
        store.persistTimeline(investigationId, timeline);
        store.verifyIntegrity(investigationId);
        store.completeInvestigation(investigationId, secondTime);

        InvestigationMetadata metadata = store.findInvestigation(investigationId);
        assertEquals(InvestigationStatus.COMPLETED, metadata.status());
        assertEquals("cluster-1", metadata.clusterId());
        assertEquals(secondTime, metadata.completedAt());
        assertEquals(SQLiteEvidenceStore.CURRENT_SCHEMA_VERSION, metadata.schemaVersion());
    }

    @Test
    void marksUnfinishedInvestigationsInterruptedAfterRestart() {
        SQLiteEvidenceStore firstProcess = newStore();
        firstProcess.initialize();
        UUID investigationId = firstProcess.createInvestigation("cluster-1", firstTime);

        SQLiteEvidenceStore restartedProcess = newStore();
        restartedProcess.initialize();
        assertEquals(1, restartedProcess.recoverInterruptedInvestigations());

        assertEquals(InvestigationStatus.INTERRUPTED,
                restartedProcess.findInvestigation(investigationId).status());
        assertEquals(0, restartedProcess.recoverInterruptedInvestigations());
    }

    @Test
    void rejectsNonContiguousObservationIndexes() {
        SQLiteEvidenceStore store = newStore();
        store.initialize();
        UUID investigationId = store.createInvestigation("cluster-1", firstTime);

        assertThrows(PersistenceException.class,
                () -> store.appendObservation(investigationId, 1,
                        snapshot(firstTime, Set.of(1), null)));
    }

    @Test
    void rejectsTimelineThatDoesNotMatchStoredObservation() {
        SQLiteEvidenceStore store = newStore();
        store.initialize();
        ClusterMetadataSnapshot first = snapshot(firstTime, Set.of(1, 2),
                new PartitionMetadataSnapshot(1, Set.of(1, 2), Set.of(1, 2)));
        ClusterMetadataSnapshot second = snapshot(secondTime, Set.of(1, 2),
                new PartitionMetadataSnapshot(2, Set.of(1, 2), Set.of(1, 2)));
        UUID investigationId = store.createInvestigation("cluster-1", firstTime);
        store.appendObservation(investigationId, 0, first);
        store.appendObservation(investigationId, 1, second);

        RecoveryTimeline validTimeline = new RecoveryTimelineReconstructor()
                .reconstruct(List.of(first, second));
        RecoveryTimeline mismatched = new RecoveryTimeline("cluster-1",
                validTimeline.entries().stream().map(entry -> entry.observationIndex() == 1
                        ? new com.recoveryguard.evidence.RecoveryTimelineEntry(
                                entry.sequence(), entry.observationIndex(), firstTime,
                                entry.occurredAt(), entry.type(), entry.orderingConfidence(),
                                entry.snapshot(), entry.event())
                        : entry).toList());

        assertThrows(PersistenceException.class,
                () -> store.persistTimeline(investigationId, mismatched));
    }

    @Test
    void detectsPayloadModification() throws Exception {
        SQLiteEvidenceStore store = newStore();
        store.initialize();
        UUID investigationId = store.createInvestigation("cluster-1", firstTime);
        store.appendObservation(investigationId, 0, snapshot(firstTime, Set.of(1), null));

        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlite:" + store.databasePath());
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER observations_no_update");
            statement.execute("UPDATE observations SET payload_json = '{}' "
                    + "WHERE investigation_id = '" + investigationId + "'");
        }

        assertThrows(PersistenceException.class, () -> store.verifyIntegrity(investigationId));
    }

    @Test
    void preventsUpdatesToCommittedObservationRows() throws Exception {
        SQLiteEvidenceStore store = newStore();
        store.initialize();
        UUID investigationId = store.createInvestigation("cluster-1", firstTime);
        store.appendObservation(investigationId, 0, snapshot(firstTime, Set.of(1), null));

        try (Connection connection = DriverManager.getConnection(
                "jdbc:sqlite:" + store.databasePath());
             Statement statement = connection.createStatement()) {
            assertThrows(java.sql.SQLException.class, () -> statement.execute(
                    "UPDATE observations SET payload_json = '{}' "
                            + "WHERE investigation_id = '" + investigationId + "'"));
        }
    }

    private SQLiteEvidenceStore newStore() {
        return new SQLiteEvidenceStore(tempDir.resolve("recoveryguard.db"), "test-version");
    }

    private static ClusterMetadataSnapshot snapshot(
            Instant observedAt, Set<Integer> brokers, PartitionMetadataSnapshot partition) {
        Map<PartitionRef, PartitionMetadataSnapshot> partitions = partition == null
                ? Map.of()
                : Map.of(new PartitionRef("orders", 0), partition);
        return new ClusterMetadataSnapshot("cluster-1", observedAt, brokers, partitions);
    }
}
