package com.recoveryguard.persistence;

import com.recoveryguard.evidence.ClusterMetadataSnapshot;
import com.recoveryguard.evidence.RecoveryTimeline;
import com.recoveryguard.evidence.RecoveryTimelineEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/**
 * SQLite-backed, append-only storage for RecoveryGuard evidence.
 *
 * <p>One instance uses one database file. Each operation opens its own JDBC
 * connection. Call recoverInterruptedInvestigations once when the application
 * starts, before accepting new work.</p>
 */
public final class SQLiteEvidenceStore {
    public static final int CURRENT_SCHEMA_VERSION = 1;

    private final Path databasePath;
    private final String jdbcUrl;
    private final String productVersion;
    private final EvidenceJsonCodec codec = new EvidenceJsonCodec();

    public SQLiteEvidenceStore(Path databasePath, String productVersion) {
        this.databasePath = Objects.requireNonNull(databasePath, "databasePath is required")
                .toAbsolutePath().normalize();
        if (productVersion == null || productVersion.isBlank()) {
            throw new IllegalArgumentException("productVersion cannot be blank");
        }
        this.productVersion = productVersion;
        this.jdbcUrl = "jdbc:sqlite:" + this.databasePath;
    }

    /** Creates the database schema. Safe to call more than once. */
    public void initialize() {
        try {
            Path parent = databasePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (Connection connection = openConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA journal_mode = WAL");
                statement.execute("PRAGMA synchronous = FULL");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS investigations (
                            investigation_id TEXT PRIMARY KEY NOT NULL,
                            cluster_id TEXT NOT NULL CHECK (length(trim(cluster_id)) > 0),
                            status TEXT NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'INTERRUPTED')),
                            created_at TEXT NOT NULL,
                            started_at TEXT NOT NULL,
                            completed_at TEXT,
                            schema_version INTEGER NOT NULL CHECK (schema_version > 0),
                            product_version TEXT NOT NULL CHECK (length(trim(product_version)) > 0),
                            CHECK (
                                (status = 'COMPLETED' AND completed_at IS NOT NULL)
                                OR (status IN ('IN_PROGRESS', 'INTERRUPTED') AND completed_at IS NULL)
                            )
                        )
                        """);
                statement.execute("""
                        CREATE INDEX IF NOT EXISTS idx_investigations_cluster_created
                        ON investigations(cluster_id, created_at)
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS observations (
                            investigation_id TEXT NOT NULL,
                            observation_index INTEGER NOT NULL CHECK (observation_index >= 0),
                            observed_at TEXT NOT NULL,
                            payload_json TEXT NOT NULL,
                            payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                            PRIMARY KEY (investigation_id, observation_index),
                            FOREIGN KEY (investigation_id)
                                REFERENCES investigations(investigation_id) ON DELETE RESTRICT
                        )
                        """);
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS timeline_entries (
                            investigation_id TEXT NOT NULL,
                            sequence INTEGER NOT NULL CHECK (sequence > 0),
                            observation_index INTEGER NOT NULL CHECK (observation_index >= 0),
                            entry_type TEXT NOT NULL CHECK (
                                entry_type IN ('SNAPSHOT', 'BROKER_EVENT', 'PARTITION_EVENT')
                            ),
                            observed_at TEXT NOT NULL,
                            occurred_at TEXT,
                            ordering_confidence TEXT NOT NULL CHECK (
                                ordering_confidence IN ('OBSERVED_ORDER', 'SAME_OBSERVATION', 'UNKNOWN_ORDER')
                            ),
                            payload_json TEXT NOT NULL,
                            payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
                            PRIMARY KEY (investigation_id, sequence),
                            FOREIGN KEY (investigation_id, observation_index)
                                REFERENCES observations(investigation_id, observation_index)
                                ON DELETE RESTRICT
                        )
                        """);
                statement.execute("""
                        CREATE INDEX IF NOT EXISTS idx_timeline_entries_observation
                        ON timeline_entries(investigation_id, observation_index, sequence)
                        """);
                statement.execute("""
                        CREATE TRIGGER IF NOT EXISTS observations_no_update
                        BEFORE UPDATE ON observations BEGIN
                            SELECT RAISE(ABORT, 'evidence observations are append-only');
                        END
                        """);
                statement.execute("""
                        CREATE TRIGGER IF NOT EXISTS observations_no_delete
                        BEFORE DELETE ON observations BEGIN
                            SELECT RAISE(ABORT, 'evidence observations are append-only');
                        END
                        """);
                statement.execute("""
                        CREATE TRIGGER IF NOT EXISTS timeline_entries_no_update
                        BEFORE UPDATE ON timeline_entries BEGIN
                            SELECT RAISE(ABORT, 'timeline evidence is append-only');
                        END
                        """);
                statement.execute("""
                        CREATE TRIGGER IF NOT EXISTS timeline_entries_no_delete
                        BEFORE DELETE ON timeline_entries BEGIN
                            SELECT RAISE(ABORT, 'timeline evidence is append-only');
                        END
                        """);
                statement.execute("""
                        CREATE TRIGGER IF NOT EXISTS investigation_status_transition
                        BEFORE UPDATE OF status ON investigations
                        WHEN NOT (OLD.status = 'IN_PROGRESS'
                                  AND NEW.status IN ('COMPLETED', 'INTERRUPTED'))
                        BEGIN
                            SELECT RAISE(ABORT, 'invalid investigation status transition');
                        END
                        """);
            }
        } catch (IOException | SQLException exception) {
            throw new PersistenceException("Could not initialize SQLite evidence database", exception);
        }
    }

    /** Creates a new investigation and returns its unique ID. */
    public UUID createInvestigation(String clusterId, Instant startedAt) {
        if (clusterId == null || clusterId.isBlank() || startedAt == null) {
            throw new IllegalArgumentException("clusterId and startedAt are required");
        }
        UUID id = UUID.randomUUID();
        Instant createdAt = Instant.now();
        String sql = """
                INSERT INTO investigations
                (investigation_id, cluster_id, status, created_at, started_at,
                 completed_at, schema_version, product_version)
                VALUES (?, ?, 'IN_PROGRESS', ?, ?, NULL, ?, ?)
                """;
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, id.toString());
            statement.setString(2, clusterId);
            statement.setString(3, createdAt.toString());
            statement.setString(4, startedAt.toString());
            statement.setInt(5, CURRENT_SCHEMA_VERSION);
            statement.setString(6, productVersion);
            statement.executeUpdate();
            return id;
        } catch (SQLException exception) {
            throw new PersistenceException("Could not create investigation", exception);
        }
    }

    /** Stores one complete raw snapshot. Observation indexes must be contiguous from zero. */
    public void appendObservation(UUID investigationId, int observationIndex,
                                  ClusterMetadataSnapshot snapshot) {
        Objects.requireNonNull(investigationId, "investigationId is required");
        Objects.requireNonNull(snapshot, "snapshot is required");
        if (observationIndex < 0) {
            throw new IllegalArgumentException("observationIndex cannot be negative");
        }
        String payload = codec.encodeSnapshot(snapshot);
        String sql = """
                INSERT INTO observations
                (investigation_id, observation_index, observed_at, payload_json, payload_sha256)
                SELECT ?, ?, ?, ?, ?
                WHERE EXISTS (
                    SELECT 1 FROM investigations
                    WHERE investigation_id = ? AND cluster_id = ? AND status = 'IN_PROGRESS'
                )
                """;
        try (Connection connection = openConnection()) {
            connection.setAutoCommit(false);
            try {
                int count;
                try (PreparedStatement query = connection.prepareStatement(
                        "SELECT COUNT(*) FROM observations WHERE investigation_id = ?")) {
                    query.setString(1, investigationId.toString());
                    try (ResultSet result = query.executeQuery()) {
                        count = result.getInt(1);
                    }
                }
                if (count != observationIndex) {
                    throw new PersistenceException("Observation index must be the next contiguous index");
                }
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, investigationId.toString());
                    statement.setInt(2, observationIndex);
                    statement.setString(3, snapshot.observedAt().toString());
                    statement.setString(4, payload);
                    statement.setString(5, sha256(payload));
                    statement.setString(6, investigationId.toString());
                    statement.setString(7, snapshot.clusterId());
                    if (statement.executeUpdate() != 1) {
                        throw new PersistenceException(
                                "Investigation does not exist, is not active, or has a different cluster ID");
                    }
                }
                connection.commit();
            } catch (RuntimeException | SQLException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (SQLException exception) {
            throw new PersistenceException("Could not store observation", exception);
        }
    }

    /** Stores the reconstructed timeline in one transaction. */
    public void persistTimeline(UUID investigationId, RecoveryTimeline timeline) {
        Objects.requireNonNull(investigationId, "investigationId is required");
        Objects.requireNonNull(timeline, "timeline is required");
        String clusterId = findClusterId(investigationId);
        if (!clusterId.equals(timeline.clusterId())) {
            throw new PersistenceException("Timeline belongs to a different Kafka cluster");
        }
        String sql = """
                INSERT INTO timeline_entries
                (investigation_id, sequence, observation_index, entry_type, observed_at,
                 occurred_at, ordering_confidence, payload_json, payload_sha256)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (Connection connection = openConnection()) {
            connection.setAutoCommit(false);
            try {
                requireInProgress(connection, investigationId);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (RecoveryTimelineEntry entry : timeline.entries()) {
                        requireObservation(connection, investigationId, entry.observationIndex(),
                                entry.observedAt());
                        String payload = codec.encodeTimelineEntry(entry);
                        statement.setString(1, investigationId.toString());
                        statement.setLong(2, entry.sequence());
                        statement.setInt(3, entry.observationIndex());
                        statement.setString(4, entry.type().name());
                        statement.setString(5, entry.observedAt().toString());
                        statement.setString(6, entry.occurredAt() == null
                                ? null : entry.occurredAt().toString());
                        statement.setString(7, entry.orderingConfidence().name());
                        statement.setString(8, payload);
                        statement.setString(9, sha256(payload));
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                connection.commit();
            } catch (RuntimeException | SQLException exception) {
                connection.rollback();
                throw exception;
            }
        } catch (SQLException exception) {
            throw new PersistenceException("Could not persist recovery timeline", exception);
        }
    }

    /** Verifies all stored observation and timeline payload hashes for one investigation. */
    public void verifyIntegrity(UUID investigationId) {
        Objects.requireNonNull(investigationId, "investigationId is required");
        verifyTable(investigationId, "observations");
        verifyTable(investigationId, "timeline_entries");
    }

    /** Completes an investigation only after all stored evidence passes integrity checks. */
    public void completeInvestigation(UUID investigationId, Instant completedAt) {
        Objects.requireNonNull(completedAt, "completedAt is required");
        verifyIntegrity(investigationId);
        String sql = """
                UPDATE investigations
                SET status = 'COMPLETED', completed_at = ?
                WHERE investigation_id = ? AND status = 'IN_PROGRESS'
                  AND EXISTS (SELECT 1 FROM observations WHERE investigation_id = ?)
                  AND EXISTS (SELECT 1 FROM timeline_entries WHERE investigation_id = ?)
                """;
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, completedAt.toString());
            statement.setString(2, investigationId.toString());
            statement.setString(3, investigationId.toString());
            statement.setString(4, investigationId.toString());
            if (statement.executeUpdate() != 1) {
                throw new PersistenceException(
                        "Investigation cannot be completed: it may be inactive or evidence may be missing");
            }
        } catch (SQLException exception) {
            throw new PersistenceException("Could not complete investigation", exception);
        }
    }

    /**
     * Marks all investigations left active by a previous process as interrupted.
     * Call once during application startup before accepting new investigations.
     */
    public int recoverInterruptedInvestigations() {
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement("""
                     UPDATE investigations SET status = 'INTERRUPTED'
                     WHERE status = 'IN_PROGRESS'
                     """)) {
            return statement.executeUpdate();
        } catch (SQLException exception) {
            throw new PersistenceException("Could not mark interrupted investigations", exception);
        }
    }

    public InvestigationMetadata findInvestigation(UUID investigationId) {
        String sql = """
                SELECT investigation_id, cluster_id, status, created_at, started_at,
                       completed_at, schema_version, product_version
                FROM investigations WHERE investigation_id = ?
                """;
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, investigationId.toString());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new PersistenceException("Investigation was not found");
                }
                String completedAt = result.getString("completed_at");
                return new InvestigationMetadata(
                        UUID.fromString(result.getString("investigation_id")),
                        result.getString("cluster_id"),
                        InvestigationStatus.valueOf(result.getString("status")),
                        Instant.parse(result.getString("created_at")),
                        Instant.parse(result.getString("started_at")),
                        completedAt == null ? null : Instant.parse(completedAt),
                        result.getInt("schema_version"),
                        result.getString("product_version"));
            }
        } catch (SQLException exception) {
            throw new PersistenceException("Could not retrieve investigation", exception);
        }
    }

    private Connection openConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA foreign_keys = ON");
            statement.execute("PRAGMA busy_timeout = 5000");
        }
        return connection;
    }

    private String findClusterId(UUID investigationId) {
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT cluster_id FROM investigations WHERE investigation_id = ?")) {
            statement.setString(1, investigationId.toString());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new PersistenceException("Investigation was not found");
                }
                return result.getString(1);
            }
        } catch (SQLException exception) {
            throw new PersistenceException("Could not read investigation", exception);
        }
    }

    private static void requireInProgress(Connection connection, UUID investigationId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM investigations WHERE investigation_id = ? AND status = 'IN_PROGRESS'")) {
            statement.setString(1, investigationId.toString());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new PersistenceException("Investigation is not active");
                }
            }
        }
    }

    private static void requireObservation(Connection connection, UUID investigationId,
                                           int observationIndex, Instant observedAt)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT observed_at FROM observations
                WHERE investigation_id = ? AND observation_index = ?
                """)) {
            statement.setString(1, investigationId.toString());
            statement.setInt(2, observationIndex);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || !observedAt.toString().equals(result.getString(1))) {
                    throw new PersistenceException(
                            "Timeline entry does not match a stored observation");
                }
            }
        }
    }

    private void verifyTable(UUID investigationId, String table) {
        if (!table.equals("observations") && !table.equals("timeline_entries")) {
            throw new IllegalArgumentException("Unsupported evidence table");
        }
        String sql = "SELECT payload_json, payload_sha256 FROM " + table
                + " WHERE investigation_id = ?";
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, investigationId.toString());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    String payload = result.getString("payload_json");
                    String expected = result.getString("payload_sha256");
                    if (!MessageDigest.isEqual(
                            expected.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                            sha256(payload).getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
                        throw new PersistenceException("Evidence integrity check failed in " + table);
                    }
                }
            }
        } catch (SQLException exception) {
            throw new PersistenceException("Could not verify evidence integrity", exception);
        }
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    public Path databasePath() {
        return databasePath;
    }
}
