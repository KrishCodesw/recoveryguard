package com.recoveryguard.observer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataSnapshotMainTest {

    @Test
    void helpDoesNotConnectToKafka() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertEquals(0, MetadataSnapshotMain.run(new String[]{"--help"},
                stream(output), stream(new ByteArrayOutputStream())));
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("--bootstrap-server"));
    }

    @Test
    void requiresBootstrapServer() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        assertEquals(2, MetadataSnapshotMain.run(new String[0],
                stream(new ByteArrayOutputStream()), stream(error)));
        assertTrue(error.toString(StandardCharsets.UTF_8).contains("--bootstrap-server is required"));
    }

    @Test
    void rejectsOutOfRangeTimeout() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        assertEquals(2, MetadataSnapshotMain.run(
                new String[]{"--bootstrap-server", "localhost:9092", "--timeout-ms", "0"},
                stream(new ByteArrayOutputStream()), stream(error)));
        assertTrue(error.toString(StandardCharsets.UTF_8).contains("between 100 and 120000"));
    }

    @Test
    void rejectsUnknownOptions() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        assertEquals(2, MetadataSnapshotMain.run(
                new String[]{"--bootstrap-server", "localhost:9092", "--write"},
                stream(new ByteArrayOutputStream()), stream(error)));
        assertTrue(error.toString(StandardCharsets.UTF_8).contains("unknown option: --write"));
    }

    private static PrintStream stream(ByteArrayOutputStream output) {
        return new PrintStream(output, true, StandardCharsets.UTF_8);
    }
}
