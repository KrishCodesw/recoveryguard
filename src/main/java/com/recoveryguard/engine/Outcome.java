package com.recoveryguard.engine;

/** Outcome of one validation check. */
public enum Outcome {
    PASS,
    VIOLATION_PROVEN,
    INCONCLUSIVE,
    ERROR,
    WARN,
    NOT_RUN
}
