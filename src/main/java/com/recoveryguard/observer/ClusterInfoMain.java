package com.recoveryguard.observer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Prints a bounded snapshot of Kafka cluster metadata.
 *
 * <p>This command uses only the Kafka Admin API. It does not create consumers,
 * read records, join consumer groups, commit offsets, or change cluster state.</p>
 */
public final class ClusterInfoMain {

    private static final int DEFAULT_TIMEOUT_MS = 10_000;
    private static final int MIN_TIMEOUT_MS = 100;
    private static final int MAX_TIMEOUT_MS = 120_000;
    private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private ClusterInfoMain() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream output, PrintStream error) {
        Options options;
        try {
            options = parseOptions(args);
        } catch (IllegalArgumentException exception) {
            error.println("Argument error: " + exception.getMessage());
            printUsage(error);
            return 2;
        }

        if (options.help()) {
            printUsage(output);
            return 0;
        }

        Properties properties = new Properties();
        if (options.clientConfig() != null) {
            try (InputStream input = Files.newInputStream(options.clientConfig())) {
                properties.load(input);
            } catch (IOException exception) {
                error.println("Could not read Kafka client config: "
                        + options.clientConfig());
                return 2;
            }
        }

        // The explicit CLI endpoint takes precedence over a value in the file.
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                options.bootstrapServers());
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG,
                "recoveryguard-cluster-info");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Integer.toString(options.timeoutMs()));
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Integer.toString(options.timeoutMs()));

        Admin admin = null;
        try {
            admin = Admin.create(properties);
            DescribeClusterResult cluster = admin.describeCluster();

            String clusterId = cluster.clusterId()
                    .get(options.timeoutMs(), TimeUnit.MILLISECONDS);
            Node controller = cluster.controller()
                    .get(options.timeoutMs(), TimeUnit.MILLISECONDS);
            Collection<Node> nodes = cluster.nodes()
                    .get(options.timeoutMs(), TimeUnit.MILLISECONDS);

            if (nodes.isEmpty()) {
                throw new IllegalStateException(
                        "Kafka returned an empty broker list");
            }

            printClusterInfo(output, clusterId, controller, nodes);
            return 0;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            error.println("Kafka observation was interrupted.");
            return 130;
        } catch (ExecutionException | TimeoutException | RuntimeException exception) {
            error.println("Kafka observation failed: " + failureMessage(exception));
            return 1;
        } finally {
            if (admin != null) {
                admin.close(ADMIN_CLOSE_TIMEOUT);
            }
        }
    }

    private static void printClusterInfo(
            PrintStream output,
            String clusterId,
            Node controller,
            Collection<Node> nodes) {

        output.println("cluster.id=" + valueOrUnknown(clusterId));
        output.println("controller.id="
                + (controller == null ? "unknown" : controller.id()));
        output.println("broker.count=" + nodes.size());

        List<Node> sortedNodes = new ArrayList<>(nodes);
        sortedNodes.sort(Comparator.comparingInt(Node::id));

        for (Node node : sortedNodes) {
            output.printf(
                    "broker.id=%d host=%s port=%d rack=%s%n",
                    node.id(),
                    node.host(),
                    node.port(),
                    valueOrUnknown(node.rack()));
        }
    }

    private static Options parseOptions(String[] args) {
        if (args == null) {
            throw new IllegalArgumentException("arguments must not be null");
        }

        String bootstrapServers = null;
        Path clientConfig = null;
        int timeoutMs = DEFAULT_TIMEOUT_MS;
        boolean help = false;

        for (int index = 0; index < args.length; index++) {
            String argument = args[index];

            switch (argument) {
                case "--help" -> help = true;
                case "--bootstrap-server" -> {
                    String value = optionValue(args, ++index, argument);
                    bootstrapServers = value;
                }
                case "--client-config" -> {
                    String value = optionValue(args, ++index, argument);
                    clientConfig = Path.of(value);
                }
                case "--timeout-ms" -> {
                    String value = optionValue(args, ++index, argument);
                    try {
                        timeoutMs = Integer.parseInt(value);
                    } catch (NumberFormatException exception) {
                        throw new IllegalArgumentException(
                                "--timeout-ms must be an integer");
                    }
                    if (timeoutMs < MIN_TIMEOUT_MS || timeoutMs > MAX_TIMEOUT_MS) {
                        throw new IllegalArgumentException(
                                "--timeout-ms must be between "
                                        + MIN_TIMEOUT_MS + " and " + MAX_TIMEOUT_MS);
                    }
                }
                default -> throw new IllegalArgumentException(
                        "unknown option: " + argument);
            }
        }

        if (!help && (bootstrapServers == null || bootstrapServers.isBlank())) {
            throw new IllegalArgumentException(
                    "--bootstrap-server is required");
        }

        return new Options(bootstrapServers, clientConfig, timeoutMs, help);
    }

    private static String optionValue(String[] args, int index, String option) {
        if (index >= args.length
                || args[index].isBlank()
                || args[index].startsWith("--")) {
            throw new IllegalArgumentException(
                    "a value is required for " + option);
        }
        return args[index];
    }

    private static String failureMessage(Exception exception) {
        Throwable cause = exception.getCause() == null
                ? exception
                : exception.getCause();
        String message = cause.getMessage();
        if (message == null || message.isBlank()) {
            return cause.getClass().getSimpleName();
        }
        return cause.getClass().getSimpleName() + ": " + message;
    }

    private static String valueOrUnknown(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }

    private static void printUsage(PrintStream output) {
        output.println("Usage: ClusterInfoMain --bootstrap-server <host:port[,host:port...]>");
        output.println("       [--client-config <properties-file>]");
        output.println("       [--timeout-ms <100..120000>]");
        output.println("       [--help]");
    }

    private record Options(
            String bootstrapServers,
            Path clientConfig,
            int timeoutMs,
            boolean help) {
    }
}
