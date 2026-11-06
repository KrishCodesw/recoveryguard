package com.recoveryguard.engine;

import java.util.List;
import java.util.Objects;

/**
 * Immutable output from the pure verdict decision function.
 *
 * Counts and check ID lists are derived by the engine.
 */
public record VerdictResult(
        Verdict verdict,
        int requiredChecksPassed,
        int requiredChecksTotal,
        List<String> provenViolationCheckIds,
        List<String> errorCheckIds,
        List<String> inconclusiveCheckIds,
        List<String> limitations) {

    public VerdictResult {
        Objects.requireNonNull(verdict, "verdict must not be null");
        if (requiredChecksPassed < 0
                || requiredChecksTotal < 0
                || requiredChecksPassed > requiredChecksTotal) {
            throw new IllegalArgumentException("invalid required check counts");
        }
        provenViolationCheckIds = List.copyOf(provenViolationCheckIds);
        errorCheckIds = List.copyOf(errorCheckIds);
        inconclusiveCheckIds = List.copyOf(inconclusiveCheckIds);
        limitations = List.copyOf(limitations);
    }
}
