package com.recoveryguard.observer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.RecoveryTimeline;
import com.recoveryguard.evidence.RecoveryTimelineEntry;
import com.recoveryguard.evidence.RecoverySignal;
import com.recoveryguard.evidence.RecoveryTimelineReconstructor;
import com.recoveryguard.persistence.InvestigationMetadata;
import com.recoveryguard.persistence.SQLiteEvidenceStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Runs a bounded, read-only metadata capture and writes an evidence report.
 *
 * <p>This command uses Kafka Admin API only. It does not read records or
 * change Kafka state. Its report describes metadata observations only.</p>
 */
public final class RecoveryCaptureMain {
    private static final int DEFAULT_OBSERVATIONS = 5;
    private static final int DEFAULT_INTERVAL_MS = 1_000;
    private static final int DEFAULT_TIMEOUT_MS = 10_000;
    private static final String PRODUCT_VERSION = "0.1.0-SNAPSHOT";

    private RecoveryCaptureMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, java.io.PrintStream output, java.io.PrintStream error) {
        Options options;
        try {
            options = parse(args);
            if (options.help()) {
                usage(output);
                return 0;
            }
        } catch (IllegalArgumentException exception) {
            error.println("Argument error: " + exception.getMessage());
            usage(error);
            return 2;
        }

        Properties properties = new Properties();
        if (options.clientConfig() != null) {
            try (InputStream input = Files.newInputStream(options.clientConfig())) {
                properties.load(input);
            } catch (IOException exception) {
                error.println("Could not read Kafka client config: " + options.clientConfig());
                return 2;
            }
        }
        properties.setProperty("bootstrap.servers", options.bootstrapServers());

        try {
            SQLiteEvidenceStore store =
                    new SQLiteEvidenceStore(options.database(), PRODUCT_VERSION);
            store.initialize();
            store.recoverInterruptedInvestigations();

            KafkaMetadataSnapshotCollector collector = new KafkaMetadataSnapshotCollector();
            ClusterMetadataSnapshot first = collector.collect(properties, options.timeoutMs());

            UUID investigationId = store.createInvestigation(first.clusterId(), first.observedAt());
            store.appendObservation(investigationId, 0, first);

            for (int index = 1; index < options.observations(); index++) {
                if (options.intervalMs() > 0) {
                    Thread.sleep(options.intervalMs());
                }
                ClusterMetadataSnapshot snapshot =
                        collector.collect(properties, options.timeoutMs());
                store.appendObservation(investigationId, index, snapshot);
            }

            List<ClusterMetadataSnapshot> snapshots = store.loadObservations(investigationId);
            RecoveryTimeline timeline = new RecoveryTimelineReconstructor().reconstruct(snapshots);
            store.persistTimeline(investigationId, timeline);
            store.verifyIntegrity(investigationId);
            store.completeInvestigation(investigationId, Instant.now());

            Report report = createReport(store.findInvestigation(investigationId), snapshots, timeline);
            ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);
            if (options.report() == null) {
                output.println(json);
            } else {
                Path parent = options.report().toAbsolutePath().normalize().getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.writeString(options.report(), json + System.lineSeparator());
                output.println("Evidence report written: " + options.report());
                output.println("investigation.id=" + investigationId);
                output.println("status=COMPLETED");
            }
            return 0;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            error.println("Metadata capture was interrupted. The investigation remains IN_PROGRESS.");
            return 130;
        } catch (Exception exception) {
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            error.println("Metadata capture failed: "
                    + cause.getClass().getSimpleName() + ": " + cause.getMessage());
            return 1;
        }
    }

    private static Report createReport(
            InvestigationMetadata metadata,
            List<ClusterMetadataSnapshot> snapshots,
            RecoveryTimeline timeline) {
        Map<RecoverySignal, Long> signalCounts = new EnumMap<>(RecoverySignal.class);
        for (RecoverySignal signal : RecoverySignal.values()) {
            signalCounts.put(signal, 0L);
        }
        for (RecoveryTimelineEntry entry : timeline.entries()) {
            if (entry.event() != null) {
                signalCounts.compute(entry.event().signal(), (signal, count) -> count + 1);
            }
        }

        ClusterMetadataSnapshot first = snapshots.get(0);
        ClusterMetadataSnapshot last = snapshots.get(snapshots.size() - 1);
        List<TimelineItem> timelineItems = timeline.entries().stream()
                .map(entry -> new TimelineItem(
                        entry.sequence(),
                        entry.observationIndex(),
                        entry.observedAt(),
                        entry.type().name(),
                        entry.orderingConfidence().name(),
                        entry.event() == null ? null : entry.event().signal().name(),
                        entry.snapshot() == null ? List.of()
                                : entry.snapshot().brokerIds().stream().sorted().toList(),
                        entry.event() == null ? List.of()
                                : entry.event().affectedBrokerIds().stream().sorted().toList(),
                        entry.event() == null ? List.of()
                                : entry.event().affectedPartitions().stream()
                                        .sorted(java.util.Comparator.comparing(
                                                com.recoveryguard.evidence.PartitionRef::topic)
                                                .thenComparingInt(
                                                com.recoveryguard.evidence.PartitionRef::partition))
                                        .map(partition -> partition.topic() + "-" + partition.partition())
                                        .toList()
                ))
                .toList();

        return new Report(
                1,
                metadata.investigationId(),
                metadata.clusterId(),
                metadata.status().name(),
                metadata.startedAt(),
                metadata.completedAt(),
                snapshots.size(),
                timeline.entries().size(),
                first.brokerIds().size(),
                last.brokerIds().size(),
                first.partitions().size(),
                last.partitions().size(),
                signalCounts,
                "VERIFIED",
                false,
                false,
                false,
                false,
                timelineItems,
                List.of(
                        "Snapshots are separate Admin API observations, not an atomic cluster view.",
                        "Metadata does not prove record integrity or acknowledged-write durability.",
                        "Timeline order does not prove causality."
                )
        );
    }

    private static Options parse(String[] args) {
        String bootstrap = null;
        Path config = null;
        Path database = Path.of("recoveryguard-evidence.db");
        Path report = null;
        int observations = DEFAULT_OBSERVATIONS;
        int interval = DEFAULT_INTERVAL_MS;
        int timeout = DEFAULT_TIMEOUT_MS;
        boolean help = false;

        for (int index = 0; index < args.length; index++) {
            String option = args[index];
            switch (option) {
                case "--help" -> help = true;
                case "--bootstrap-server" -> bootstrap = value(args, ++index, option);
                case "--client-config" -> config = Path.of(value(args, ++index, option));
                case "--database" -> database = Path.of(value(args, ++index, option));
                case "--report" -> report = Path.of(value(args, ++index, option));
                case "--observations" -> observations = integer(args, ++index, option);
                case "--interval-ms" -> interval = integer(args, ++index, option);
                case "--timeout-ms" -> timeout = integer(args, ++index, option);
                default -> throw new IllegalArgumentException("unknown option: " + option);
            }
        }

        if (help) {
            return new Options(bootstrap, config, database, report, observations, interval, timeout, true);
        }
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalArgumentException("--bootstrap-server is required");
        }
        if (observations < 2 || observations > 10_000) {
            throw new IllegalArgumentException("--observations must be between 2 and 10000");
        }
        if (interval < 0 || interval > 600_000) {
            throw new IllegalArgumentException("--interval-ms must be between 0 and 600000");
        }
        if (timeout < 100 || timeout > 120_000) {
            throw new IllegalArgumentException("--timeout-ms must be between 100 and 120000");
        }
        return new Options(bootstrap, config, database, report, observations, interval, timeout, false);
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length || args[index].isBlank() || args[index].startsWith("--")) {
            throw new IllegalArgumentException("a value is required for " + option);
        }
        return args[index];
    }

    private static int integer(String[] args, int index, String option) {
        try {
            return Integer.parseInt(value(args, index, option));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(option + " must be an integer");
        }
    }

    private static void usage(java.io.PrintStream output) {
        output.println("Usage: RecoveryCaptureMain --bootstrap-server <host:port[,host:port...]>");
        output.println("       [--client-config <properties-file>] [--database <sqlite-file>]");
        output.println("       [--observations <2..10000>] [--interval-ms <0..600000>]");
        output.println("       [--timeout-ms <100..120000>] [--report <json-file>] [--help]");
    }

    private record Options(
            String bootstrapServers,
            Path clientConfig,
            Path database,
            Path report,
            int observations,
            int intervalMs,
            int timeoutMs,
            boolean help) {
    }

    private record Report(
            int schemaVersion,
            UUID investigationId,
            String clusterId,
            String status,
            Instant startedAt,
            Instant completedAt,
            int observationCount,
            int timelineEntryCount,
            int initialBrokerCount,
            int finalBrokerCount,
            int initialPartitionCount,
            int finalPartitionCount,
            Map<RecoverySignal, Long> detectedSignalCounts,
            String evidenceIntegrity,
            boolean recordsRead,
            boolean consumerGroupJoined,
            boolean offsetsCommitted,
            boolean kafkaStateChanged,
            List<TimelineItem> timeline,
            List<String> limitations) {
    }

    private record TimelineItem(
            long sequence,
            int observationIndex,
            Instant observedAt,
            String type,
            String orderingConfidence,
            String signal,
            List<Integer> observedBrokerIds,
            List<Integer> affectedBrokerIds,
            List<String> affectedPartitions) {
    }
}
