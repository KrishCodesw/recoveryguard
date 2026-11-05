package com.recoveryguard.engine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Pure verdict decision function.
 *
 * No Kafka, storage, clock, logging, environment, network, or mutable global
 * dependencies are used. The same input always produces the same output or
 * the same OPEN-rule exception.
 */
public final class VerdictEngine {

    private static final String OPEN_ERROR_INCONCLUSIVE =
            "OPEN-01_ERROR_VS_INCONCLUSIVE_PRECEDENCE";
    private static final String OPEN_EMPTY_CHECK_LIST =
            "OPEN-02_EMPTY_CHECK_LIST";
    private static final String OPEN_REQUIRED_INTEGRITY_PROOF =
            "OPEN-03_REQUIRED_INTEGRITY_PROOF_GATE";

    private VerdictEngine() {
    }

    public static VerdictResult decide(List<CheckResult> inputChecks) {
        Objects.requireNonNull(inputChecks, "inputChecks must not be null");
        List<CheckResult> checks = List.copyOf(inputChecks);

        if (checks.isEmpty()) {
            throw new OpenDecisionException(
                    OPEN_EMPTY_CHECK_LIST,
                    "OPEN-02: verdict semantics for an empty check list are undecided");
        }

        ensureUniqueCheckIds(checks);

        List<String> provenViolations = sortedIds(checks, Outcome.VIOLATION_PROVEN);
        List<CheckResult> requiredChecks = checks.stream()
                .filter(CheckResult::required)
                .toList();

        // A proven violation must not be hidden by another check's error.
        if (!provenViolations.isEmpty()) {
            return result(
                    Verdict.DATA_INTEGRITY_FAILURE,
                    checks,
                    requiredChecks,
                    provenViolations);
        }

        boolean hasRequiredError = requiredChecks.stream()
                .anyMatch(check -> check.outcome() == Outcome.ERROR);
        boolean hasRequiredIncomplete = requiredChecks.stream()
                .anyMatch(check -> check.outcome() == Outcome.INCONCLUSIVE
                        || check.outcome() == Outcome.NOT_RUN);

        if (hasRequiredError && hasRequiredIncomplete) {
            throw new OpenDecisionException(
                    OPEN_ERROR_INCONCLUSIVE,
                    "OPEN-01: precedence between required ERROR and INCONCLUSIVE/NOT_RUN "
                            + "checks is undecided");
        }

        if (hasRequiredError) {
            return result(Verdict.VALIDATION_ERROR, checks, requiredChecks, List.of());
        }

        if (hasRequiredIncomplete) {
            return result(Verdict.INCONCLUSIVE, checks, requiredChecks, List.of());
        }

        boolean hasRequiredIntegrityProof = requiredChecks.stream()
                .anyMatch(check -> check.kind() == Kind.INTEGRITY_PROOF);

        if (!hasRequiredIntegrityProof) {
            throw new OpenDecisionException(
                    OPEN_REQUIRED_INTEGRITY_PROOF,
                    "OPEN-03: it is undecided whether RECOVERY_VALIDATED requires "
                            + "at least one required INTEGRITY_PROOF check");
        }

        boolean allRequiredChecksPassed = requiredChecks.stream()
                .allMatch(check -> check.outcome() == Outcome.PASS);

        if (!allRequiredChecksPassed) {
            throw new IllegalArgumentException(
                    "unsupported required check outcome combination");
        }

        return result(Verdict.RECOVERY_VALIDATED, checks, requiredChecks, List.of());
    }

    private static void ensureUniqueCheckIds(List<CheckResult> checks) {
        Set<String> ids = new HashSet<>();
        for (CheckResult check : checks) {
            if (!ids.add(check.checkId())) {
                throw new IllegalArgumentException("duplicate checkId: " + check.checkId());
            }
        }
    }

    private static List<String> sortedIds(List<CheckResult> checks, Outcome outcome) {
        return checks.stream()
                .filter(check -> check.outcome() == outcome)
                .map(CheckResult::checkId)
                .sorted()
                .toList();
    }

    private static VerdictResult result(
            Verdict verdict,
            List<CheckResult> allChecks,
            List<CheckResult> requiredChecks,
            List<String> provenViolations) {

        int requiredPassed = (int) requiredChecks.stream()
                .filter(check -> check.outcome() == Outcome.PASS)
                .count();

        List<String> errors = sortedIds(allChecks, Outcome.ERROR);
        List<String> inconclusive = allChecks.stream()
                .filter(check -> check.outcome() == Outcome.INCONCLUSIVE
                        || check.outcome() == Outcome.NOT_RUN)
                .map(CheckResult::checkId)
                .sorted()
                .toList();

        List<String> limitations = new ArrayList<>();
        allChecks.forEach(check -> limitations.addAll(check.limitations()));
        limitations = limitations.stream().distinct().sorted().toList();

        return new VerdictResult(
                verdict,
                requiredPassed,
                requiredChecks.size(),
                provenViolations,
                errors,
                inconclusive,
                limitations);
    }
}
