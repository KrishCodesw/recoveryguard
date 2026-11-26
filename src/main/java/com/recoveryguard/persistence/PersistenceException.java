package com.recoveryguard.persistence;

/** Raised when evidence cannot be stored or verified. */
public final class PersistenceException extends RuntimeException {
    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistenceException(String message) {
        super(message);
    }
}
