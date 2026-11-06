package com.recoveryguard.engine;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerdictEngineDecisionTableTest {

    @Test
    void provenViolationWinsOverRequiredErrorAndIncompleteEvidence() {
        List<CheckResult> checks = List.of(
                check("required-error", Kind.EVIDENCE_GATE, true, Outcome.ERROR),
                check("required-incomplete", Kind.EVIDENCE_GATE, true, Outcome.INCONCLUSIVE),
                check("proven-violation", Kind.INTEGRITY_PROOF, false,
                        Outcome.VIOLATION_PROVEN, "evidence-1"));

        VerdictResult result = VerdictEngine.decide(checks);

        assertEquals(Verdict.DATA_INTEGRITY_FAILURE, result.verdict());
        assertEquals(List.of("proven-violation"), result.provenViolationCheckIds());
        assertEquals(List.of("required-error"), result.errorCheckIds());
    }

    @Test
    void requiredErrorWithoutProvenViolationReturnsValidationError() {
        VerdictResult result = VerdictEngine.decide(List.of(
                check("integrity-pass", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "evidence-1"),
                check("collection-error", Kind.EVIDENCE_GATE, true, Outcome.ERROR)));

        assertEquals(Verdict.VALIDATION_ERROR, result.verdict());
    }

    @Test
    void requiredInconclusiveWithoutErrorReturnsInconclusive() {
        VerdictResult result = VerdictEngine.decide(List.of(
                check("integrity-pass", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "evidence-1"),
                check("freshness-gate", Kind.EVIDENCE_GATE, true, Outcome.INCONCLUSIVE)));

        assertEquals(Verdict.INCONCLUSIVE, result.verdict());
    }

    @Test
    void requiredNotRunWithoutErrorReturnsInconclusive() {
        VerdictResult result = VerdictEngine.decide(List.of(
                check("integrity-pass", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "evidence-1"),
                check("not-run", Kind.EVIDENCE_GATE, true, Outcome.NOT_RUN)));

        assertEquals(Verdict.INCONCLUSIVE, result.verdict());
    }

    @Test
    void allRequiredChecksPassWithRequiredIntegrityProofReturnsValidated() {
        VerdictResult result = VerdictEngine.decide(List.of(
                check("integrity-pass", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "evidence-1"),
                check("evidence-gate-pass", Kind.EVIDENCE_GATE, true, Outcome.PASS)));

        assertEquals(Verdict.RECOVERY_VALIDATED, result.verdict());
        assertEquals(2, result.requiredChecksPassed());
        assertEquals(2, result.requiredChecksTotal());
    }

    @Test
    void optionalAdvisoryOutcomesDoNotBlockValidatedVerdict() {
        for (Outcome outcome : List.of(
                Outcome.WARN, Outcome.ERROR, Outcome.INCONCLUSIVE, Outcome.NOT_RUN, Outcome.PASS)) {
            VerdictResult result = VerdictEngine.decide(List.of(
                    check("integrity-pass", Kind.INTEGRITY_PROOF, true,
                            Outcome.PASS, "evidence-1"),
                    check("optional-advisory", Kind.ADVISORY, false, outcome)));

            assertEquals(Verdict.RECOVERY_VALIDATED, result.verdict(), outcome.name());
        }
    }

    @Test
    void optionalIntegrityCheckCanStillProveViolation() {
        VerdictResult result = VerdictEngine.decide(List.of(
                check("required-integrity-pass", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "evidence-1"),
                check("optional-integrity-violation", Kind.INTEGRITY_PROOF, false,
                        Outcome.VIOLATION_PROVEN, "evidence-2")));

        assertEquals(Verdict.DATA_INTEGRITY_FAILURE, result.verdict());
    }

    @Test
    void emptyCheckListRemainsOpen() {
        OpenDecisionException exception = assertThrows(
                OpenDecisionException.class,
                () -> VerdictEngine.decide(List.of()));

        assertEquals("OPEN-02_EMPTY_CHECK_LIST", exception.ruleId());
    }

    @Test
    void requiredErrorAndInconclusivePrecedenceRemainsOpen() {
        OpenDecisionException exception = assertThrows(
                OpenDecisionException.class,
                () -> VerdictEngine.decide(List.of(
                        check("integrity-pass", Kind.INTEGRITY_PROOF, true,
                                Outcome.PASS, "evidence-1"),
                        check("required-error", Kind.EVIDENCE_GATE, true, Outcome.ERROR),
                        check("required-incomplete", Kind.EVIDENCE_GATE, true,
                                Outcome.INCONCLUSIVE))));

        assertEquals("OPEN-01_ERROR_VS_INCONCLUSIVE_PRECEDENCE", exception.ruleId());
    }

    @Test
    void positiveVerdictWithoutRequiredIntegrityProofRemainsOpen() {
        OpenDecisionException exception = assertThrows(
                OpenDecisionException.class,
                () -> VerdictEngine.decide(List.of(
                        check("evidence-gate-pass", Kind.EVIDENCE_GATE, true, Outcome.PASS),
                        check("optional-advisory", Kind.ADVISORY, false, Outcome.WARN))));

        assertEquals("OPEN-03_REQUIRED_INTEGRITY_PROOF_GATE", exception.ruleId());
    }

    @Test
    void requiredAdvisoryCheckIsRejectedAtConstruction() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> check("invalid-required-advisory", Kind.ADVISORY, true, Outcome.PASS));

        assertTrue(exception.getMessage().contains("ADVISORY"));
    }

    private static CheckResult check(
            String id, Kind kind, boolean required, Outcome outcome, String... evidenceRefs) {
        return new CheckResult(id, kind, required, outcome,
                List.of(evidenceRefs), List.of());
    }
}
