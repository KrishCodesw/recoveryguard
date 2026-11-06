package com.recoveryguard.observer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicPartitionInfoMainTest {
    @Test
    void helpDoesNotRequireKafkaConnection() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int code = TopicPartitionInfoMain.run(new String[]{"--help"},
                stream(output), stream(new ByteArrayOutputStream()));
        assertEquals(0, code);
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("--bootstrap-server"));
    }

    @Test
    void bootstrapServerIsRequired() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int code = TopicPartitionInfoMain.run(new String[0],
                stream(new ByteArrayOutputStream()), stream(error));
        assertEquals(2, code);
        assertTrue(error.toString(StandardCharsets.UTF_8).contains("--bootstrap-server is required"));
    }

    @Test
    void unknownOptionsAreRejected() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int code = TopicPartitionInfoMain.run(
                new String[]{"--bootstrap-server", "localhost:9092", "--consumer"},
                stream(new ByteArrayOutputStream()), stream(error));
        assertEquals(2, code);
        assertTrue(error.toString(StandardCharsets.UTF_8).contains("unknown option: --consumer"));
    }

    @Test
    void timeoutMustStayWithinConfiguredBounds() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int code = TopicPartitionInfoMain.run(
                new String[]{"--bootstrap-server", "localhost:9092", "--timeout-ms", "0"},
                stream(new ByteArrayOutputStream()), stream(error));
        assertEquals(2, code);
        assertTrue(error.toString(StandardCharsets.UTF_8)
                .contains("--timeout-ms must be between 100 and 120000"));
    }

    @Test
    void missingClientConfigFailsBeforeConnecting() {
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int code = TopicPartitionInfoMain.run(
                new String[]{"--bootstrap-server", "localhost:9092",
                        "--client-config", "missing-kafka-client.properties"},
                stream(new ByteArrayOutputStream()), stream(error));
        assertEquals(2, code);
        assertTrue(error.toString(StandardCharsets.UTF_8)
                .contains("Could not read Kafka client config"));
    }

    private static PrintStream stream(ByteArrayOutputStream output) {
        return new PrintStream(output, true, StandardCharsets.UTF_8);
    }
}
