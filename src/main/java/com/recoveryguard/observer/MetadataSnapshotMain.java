package com.recoveryguard.observer;

import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.PartitionRef;
import com.recoveryguard.evidence.PartitionMetadataSnapshot;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Properties;

/** Command-line entry point for one read-only Kafka metadata snapshot. */
public final class MetadataSnapshotMain {
    private static final int DEFAULT_TIMEOUT_MS = 10_000;

    private MetadataSnapshotMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream output, PrintStream error) {
        String bootstrapServers = null;
        Path clientConfig = null;
        int timeoutMs = DEFAULT_TIMEOUT_MS;

        try {
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--help" -> {
                        usage(output);
                        return 0;
                    }
                    case "--bootstrap-server" -> bootstrapServers = value(args, ++index, "--bootstrap-server");
                    case "--client-config" -> clientConfig = Path.of(value(args, ++index, "--client-config"));
                    case "--timeout-ms" -> {
                        try {
                            timeoutMs = Integer.parseInt(value(args, ++index, "--timeout-ms"));
                        } catch (NumberFormatException exception) {
                            throw new IllegalArgumentException("--timeout-ms must be an integer");
                        }
                    }
                    default -> throw new IllegalArgumentException("unknown option: " + args[index]);
                }
            }
            if (bootstrapServers == null || bootstrapServers.isBlank()) {
                throw new IllegalArgumentException("--bootstrap-server is required");
            }
            if (timeoutMs < 100 || timeoutMs > 120_000) {
                throw new IllegalArgumentException("--timeout-ms must be between 100 and 120000");
            }
        } catch (IllegalArgumentException exception) {
            error.println("Argument error: " + exception.getMessage());
            usage(error);
            return 2;
        }

        Properties properties = new Properties();
        if (clientConfig != null) {
            try (InputStream input = Files.newInputStream(clientConfig)) {
                properties.load(input);
            } catch (IOException exception) {
                error.println("Could not read Kafka client config: " + clientConfig);
                return 2;
            }
        }
        properties.setProperty("bootstrap.servers", bootstrapServers);

        try {
            ClusterMetadataSnapshot snapshot =
                    new KafkaMetadataSnapshotCollector().collect(properties, timeoutMs);
            print(snapshot, output);
            return 0;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            error.println("Kafka metadata collection was interrupted.");
            return 130;
        } catch (Exception exception) {
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            error.println("Kafka metadata collection failed: "
                    + cause.getClass().getSimpleName() + ": " + cause.getMessage());
            return 1;
        }
    }

    private static void print(ClusterMetadataSnapshot snapshot, PrintStream output) {
        output.println("cluster.id=" + snapshot.clusterId());
        output.println("observed.at=" + snapshot.observedAt());
        output.println("broker.count=" + snapshot.brokerIds().size());
        snapshot.brokerIds().stream().sorted()
                .forEach(id -> output.println("broker.id=" + id));
        output.println("partition.count=" + snapshot.partitions().size());
        snapshot.partitions().entrySet().stream()
                .sorted(Comparator.comparing((java.util.Map.Entry<PartitionRef, PartitionMetadataSnapshot> entry)
                                -> entry.getKey().topic())
                        .thenComparingInt(entry -> entry.getKey().partition()))
                .forEach(entry -> {
                    var partition = entry.getValue();
                    output.printf("partition.topic=%s id=%d leader=%s replicas=%s isr=%s%n",
                            entry.getKey().topic(),
                            entry.getKey().partition(),
                            partition.leaderId() == null ? "none" : partition.leaderId(),
                            partition.replicaIds().stream().sorted().map(String::valueOf)
                                    .collect(java.util.stream.Collectors.joining(",")),
                            partition.isrIds().stream().sorted().map(String::valueOf)
                                    .collect(java.util.stream.Collectors.joining(",")));
                });
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length || args[index].isBlank() || args[index].startsWith("--")) {
            throw new IllegalArgumentException("a value is required for " + option);
        }
        return args[index];
    }

    private static void usage(PrintStream output) {
        output.println("Usage: MetadataSnapshotMain --bootstrap-server <host:port[,host:port...]>");
        output.println("       [--client-config <properties-file>] [--timeout-ms <100..120000>] [--help]");
    }
}
