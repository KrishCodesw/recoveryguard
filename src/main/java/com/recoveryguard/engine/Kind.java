package com.recoveryguard.engine;

/** Role of a check in the validation model. */
public enum Kind {
    /** Evaluates a declared correctness invariant and may prove a violation. */
    INTEGRITY_PROOF,

    /** Evaluates whether required evidence is available and usable. */
    EVIDENCE_GATE,

    /** Adds context but does not block a positive verdict by itself. */
    ADVISORY
}
