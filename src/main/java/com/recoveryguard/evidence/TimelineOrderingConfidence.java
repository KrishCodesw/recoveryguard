package com.recoveryguard.evidence;

import java.time.Instant;

/**
 * Describes the strength of the ordering evidence for a timeline event.
 */
public enum TimelineOrderingConfidence {
    /** The event belongs to an observation with a unique observation timestamp. */
    OBSERVED_ORDER,

    /** Multiple events were derived from the same metadata observation. */
    SAME_OBSERVATION,

    /** More than one observation has the same timestamp; temporal order is unknown. */
    UNKNOWN_ORDER
}
