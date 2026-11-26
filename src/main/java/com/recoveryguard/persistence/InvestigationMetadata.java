package com.recoveryguard.persistence;

import java.time.Instant;
import java.util.UUID;

/** Immutable metadata for one RecoveryGuard investigation. */
public record InvestigationMetadata(
        UUID investigationId,
        String clusterId,
        InvestigationStatus status,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        int schemaVersion,
        String productVersion
) {
    public InvestigationMetadata {
        if (investigationId == null || clusterId == null || clusterId.isBlank()
                || status == null || createdAt == null || startedAt == null
                || schemaVersion < 1 || productVersion == null || productVersion.isBlank()) {
            throw new IllegalArgumentException("investigation metadata is incomplete");
        }
        if (status == InvestigationStatus.COMPLETED && completedAt == null) {
            throw new IllegalArgumentException("completed investigation requires completedAt");
        }
        if (status != InvestigationStatus.COMPLETED && completedAt != null) {
            throw new IllegalArgumentException("only completed investigations can have completedAt");
        }
    }
}
