package com.recoveryguard.observer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveryCaptureMainTest {
    @TempDir
    Path tempDir;

    @Test
    void helpPrintsUsageWithoutConnectingToKafka() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = RecoveryCaptureMain.run(
                new String[]{"--help"},
                new PrintStream(output),
                new PrintStream(error));

        assertEquals(0, exitCode);
        assertTrue(output.toString().contains("RecoveryCaptureMain"));
        assertEquals("", error.toString());
    }

    @Test
    void rejectsMissingBootstrapServer() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = RecoveryCaptureMain.run(
                new String[]{"--database", tempDir.resolve("test.db").toString()},
                new PrintStream(output),
                new PrintStream(error));

        assertEquals(2, exitCode);
        assertTrue(error.toString().contains("--bootstrap-server is required"));
    }

    @Test
    void rejectsObservationCountOutsideSupportedRange() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        int exitCode = RecoveryCaptureMain.run(
                new String[]{
                        "--bootstrap-server", "localhost:29092",
                        "--observations", "1"
                },
                new PrintStream(output),
                new PrintStream(error));

        assertEquals(2, exitCode);
        assertTrue(error.toString().contains("--observations must be between 2 and 10000"));
    }
}
