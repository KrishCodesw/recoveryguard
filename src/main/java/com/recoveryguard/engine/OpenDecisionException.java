package com.recoveryguard.engine;

/**
 * Signals an input that reaches a verdict rule deliberately marked OPEN.
 *
 * This is a development guard, not a RecoveryGuard verdict.
 */
public final class OpenDecisionException extends IllegalStateException {

    private final String ruleId;

    public OpenDecisionException(String ruleId, String message) {
        super(message);
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must not be blank");
        }
        this.ruleId = ruleId;
    }

    public String ruleId() {
        return ruleId;
    }
}
