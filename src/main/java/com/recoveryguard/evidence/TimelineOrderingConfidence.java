package com.recoveryguard.evidence;

/**
 * Describes the strength of the ordering evidence for a timeline entry.
 */
public enum TimelineOrderingConfidence {
    /** The observation has a unique timestamp in this timeline. */
    OBSERVED_ORDER,

    /** Multiple entries belong to the same metadata observation. */
    SAME_OBSERVATION,

    /** Multiple observations share a timestamp; their temporal order is unknown. */
    UNKNOWN_ORDER
}
