package com.recoveryguard.engine;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VerdictEnginePropertiesTest {

    @Test
    void provenViolationDominatesOtherResultsForEveryInputOrder() {
        List<CheckResult> base = List.of(
                check("proof-pass", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "proof-evidence"),
                check("required-error", Kind.EVIDENCE_GATE, true, Outcome.ERROR),
                check("required-inconclusive", Kind.EVIDENCE_GATE, true,
                        Outcome.INCONCLUSIVE),
                check("optional-violation", Kind.INTEGRITY_PROOF, false,
                        Outcome.VIOLATION_PROVEN, "violation-evidence"));

        for (List<CheckResult> permutation : permutations(base)) {
            assertEquals(Verdict.DATA_INTEGRITY_FAILURE,
                    VerdictEngine.decide(permutation).verdict());
        }
    }

    @Test
    void decisionIsInvariantToInputOrderForClosedCases() {
        List<CheckResult> base = List.of(
                check("required-proof", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "proof-evidence"),
                check("required-gate", Kind.EVIDENCE_GATE, true, Outcome.PASS),
                check("advisory-warn", Kind.ADVISORY, false, Outcome.WARN),
                check("advisory-error", Kind.ADVISORY, false, Outcome.ERROR));

        Verdict expected = VerdictEngine.decide(base).verdict();
        for (List<CheckResult> permutation : permutations(base)) {
            assertEquals(expected, VerdictEngine.decide(permutation).verdict());
        }
    }

    @Test
    void optionalAdvisoryNoiseNeverChangesValidatedVerdict() {
        List<Outcome> optionalOutcomes = List.of(
                Outcome.PASS, Outcome.WARN, Outcome.ERROR,
                Outcome.INCONCLUSIVE, Outcome.NOT_RUN);

        for (Outcome outcome : optionalOutcomes) {
            VerdictResult result = VerdictEngine.decide(List.of(
                    check("required-proof", Kind.INTEGRITY_PROOF, true,
                            Outcome.PASS, "proof-evidence"),
                    check("optional", Kind.ADVISORY, false, outcome)));

            assertEquals(Verdict.RECOVERY_VALIDATED, result.verdict());
        }
    }

    @Test
    void exhaustiveRequiredOutcomeTableMatchesClosedRules() {
        Outcome[] outcomes = {
                Outcome.PASS, Outcome.ERROR, Outcome.INCONCLUSIVE, Outcome.NOT_RUN
        };

        for (Outcome proof : outcomes) {
            for (Outcome gateA : outcomes) {
                for (Outcome gateB : outcomes) {
                    List<Outcome> selected = List.of(proof, gateA, gateB);
                    boolean hasError = selected.contains(Outcome.ERROR);
                    boolean hasIncomplete = selected.contains(Outcome.INCONCLUSIVE)
                            || selected.contains(Outcome.NOT_RUN);

                    List<CheckResult> checks = List.of(
                            check("required-proof", Kind.INTEGRITY_PROOF, true,
                                    proof, "proof-evidence"),
                            check("required-gate-a", Kind.EVIDENCE_GATE, true, gateA),
                            check("required-gate-b", Kind.EVIDENCE_GATE, true, gateB));

                    if (hasError && hasIncomplete) {
                        assertThrows(OpenDecisionException.class,
                                () -> VerdictEngine.decide(checks));
                    } else if (hasError) {
                        assertEquals(Verdict.VALIDATION_ERROR,
                                VerdictEngine.decide(checks).verdict());
                    } else if (hasIncomplete) {
                        assertEquals(Verdict.INCONCLUSIVE,
                                VerdictEngine.decide(checks).verdict());
                    } else {
                        assertEquals(Verdict.RECOVERY_VALIDATED,
                                VerdictEngine.decide(checks).verdict());
                    }
                }
            }
        }
    }

    @Test
    void violationResultIsIndependentOfOptionalAndRequiredErrorNoise() {
        List<CheckResult> base = List.of(
                check("required-proof", Kind.INTEGRITY_PROOF, true,
                        Outcome.PASS, "proof-evidence"),
                check("optional-advisory", Kind.ADVISORY, false, Outcome.ERROR),
                check("optional-violation", Kind.INTEGRITY_PROOF, false,
                        Outcome.VIOLATION_PROVEN, "violation-evidence"));

        for (int index = 0; index < 20; index++) {
            List<CheckResult> shuffled = new ArrayList<>(base);
            Collections.rotate(shuffled, index % shuffled.size());
            assertEquals(Verdict.DATA_INTEGRITY_FAILURE,
                    VerdictEngine.decide(shuffled).verdict());
        }
    }

    private static List<List<CheckResult>> permutations(List<CheckResult> values) {
        List<List<CheckResult>> result = new ArrayList<>();
        permute(new ArrayList<>(values), 0, result);
        return result;
    }

    private static void permute(
            List<CheckResult> values, int index, List<List<CheckResult>> result) {
        if (index == values.size()) {
            result.add(List.copyOf(values));
            return;
        }
        for (int current = index; current < values.size(); current++) {
            Collections.swap(values, index, current);
            permute(values, index + 1, result);
            Collections.swap(values, index, current);
        }
    }

    private static CheckResult check(
            String id, Kind kind, boolean required, Outcome outcome, String... evidenceRefs) {
        return new CheckResult(id, kind, required, outcome,
                List.of(evidenceRefs), List.of());
    }
}
