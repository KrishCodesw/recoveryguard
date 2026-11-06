package com.recoveryguard.engine;

import java.util.List;
import java.util.Objects;

/**
 * Immutable result from one validation check.
 *
 * @param checkId stable identifier for the check
 * @param kind role of the check
 * @param required whether this check is required for a positive verdict
 * @param outcome result produced by the check
 * @param evidenceRefs evidence identifiers supporting the result
 * @param limitations known limits of this check result
 */
public record CheckResult(
        String checkId,
        Kind kind,
        boolean required,
        Outcome outcome,
        List<String> evidenceRefs,
        List<String> limitations) {

    public CheckResult {
        if (checkId == null || checkId.isBlank()) {
            throw new IllegalArgumentException("checkId must not be blank");
        }
        Objects.requireNonNull(kind, "kind must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        evidenceRefs = immutableNonBlankList(evidenceRefs, "evidenceRefs");
        limitations = immutableNonBlankList(limitations, "limitations");

        if (kind == Kind.ADVISORY && required) {
            throw new IllegalArgumentException("ADVISORY checks must be optional");
        }
        if (outcome == Outcome.WARN && (kind != Kind.ADVISORY || required)) {
            throw new IllegalArgumentException("WARN is reserved for optional ADVISORY checks");
        }
        if (outcome == Outcome.VIOLATION_PROVEN) {
            if (kind != Kind.INTEGRITY_PROOF) {
                throw new IllegalArgumentException(
                        "VIOLATION_PROVEN is valid only for INTEGRITY_PROOF checks");
            }
            if (evidenceRefs.isEmpty()) {
                throw new IllegalArgumentException(
                        "VIOLATION_PROVEN requires at least one evidence reference");
            }
        }
    }

    private static List<String> immutableNonBlankList(List<String> values, String fieldName) {
        Objects.requireNonNull(values, fieldName + " must not be null");
        List<String> copy = List.copyOf(values);
        if (copy.stream().anyMatch(value -> value.isBlank())) {
            throw new IllegalArgumentException(fieldName + " must not contain blank values");
        }
        return copy;
    }
}
