PRAGMA foreign_keys = ON;

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
);

CREATE INDEX IF NOT EXISTS idx_investigations_cluster_created
    ON investigations(cluster_id, created_at);

CREATE TABLE IF NOT EXISTS observations (
    investigation_id TEXT NOT NULL,
    observation_index INTEGER NOT NULL CHECK (observation_index >= 0),
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    PRIMARY KEY (investigation_id, observation_index),
    FOREIGN KEY (investigation_id)
        REFERENCES investigations(investigation_id)
        ON DELETE RESTRICT
);

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
);

CREATE INDEX IF NOT EXISTS idx_timeline_entries_observation
    ON timeline_entries(investigation_id, observation_index, sequence);

CREATE TRIGGER IF NOT EXISTS observations_no_update
BEFORE UPDATE ON observations
BEGIN
    SELECT RAISE(ABORT, 'evidence observations are append-only');
END;

CREATE TRIGGER IF NOT EXISTS observations_no_delete
BEFORE DELETE ON observations
BEGIN
    SELECT RAISE(ABORT, 'evidence observations are append-only');
END;

CREATE TRIGGER IF NOT EXISTS timeline_entries_no_update
BEFORE UPDATE ON timeline_entries
BEGIN
    SELECT RAISE(ABORT, 'timeline evidence is append-only');
END;

CREATE TRIGGER IF NOT EXISTS timeline_entries_no_delete
BEFORE DELETE ON timeline_entries
BEGIN
    SELECT RAISE(ABORT, 'timeline evidence is append-only');
END;

CREATE TRIGGER IF NOT EXISTS investigation_status_transition
BEFORE UPDATE OF status ON investigations
WHEN NOT (
    OLD.status = 'IN_PROGRESS'
    AND NEW.status IN ('COMPLETED', 'INTERRUPTED')
)
BEGIN
    SELECT RAISE(ABORT, 'invalid investigation status transition');
END;
