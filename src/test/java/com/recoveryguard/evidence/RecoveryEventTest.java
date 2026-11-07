package com.recoveryguard.evidence;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryEventTest {

    @Test
    void createsValidRecoveryEvent() {
        PartitionRef partition = new PartitionRef("orders", 0);

        RecoveryEvent event = new RecoveryEvent(
                UUID.randomUUID(),
                "cluster-1",
                Instant.now(),
                RecoverySignal.LEADER_CHANGED,
                Set.of(partition)
        );

        assertEquals("cluster-1", event.clusterId());
        assertEquals(RecoverySignal.LEADER_CHANGED, event.signal());
        assertTrue(event.affectedPartitions().contains(partition));
    }

    @Test
    void rejectsBlankClusterId() {
        assertThrows(IllegalArgumentException.class,
                () -> new RecoveryEvent(
                        UUID.randomUUID(),
                        " ",
                        Instant.now(),
                        RecoverySignal.BROKER_AVAILABLE,
                        Set.of()
                ));
    }

    @Test
    void rejectsNullEventId() {
        assertThrows(IllegalArgumentException.class,
                () -> new RecoveryEvent(
                        null,
                        "cluster-1",
                        Instant.now(),
                        RecoverySignal.BROKER_AVAILABLE,
                        Set.of()
                ));
    }

    @Test
    void affectedPartitionsAreImmutable() {
        Set<PartitionRef> partitions = new java.util.HashSet<>();
        partitions.add(new PartitionRef("orders", 0));

        RecoveryEvent event = new RecoveryEvent(
                UUID.randomUUID(),
                "cluster-1",
                Instant.now(),
                RecoverySignal.LEADER_CHANGED,
                partitions
        );

        partitions.clear();

        assertEquals(1, event.affectedPartitions().size());
    }

    @Test
    void rejectsNegativePartitionNumber() {
        assertThrows(IllegalArgumentException.class,
                () -> new PartitionRef("orders", -1));
    }
}
