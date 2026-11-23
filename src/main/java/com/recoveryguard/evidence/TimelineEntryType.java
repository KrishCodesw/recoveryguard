package com.recoveryguard.evidence;

/** The two kinds of evidence represented in a reconstructed timeline. */
public enum TimelineEntryType {
    SNAPSHOT,
    BROKER_EVENT,
    PARTITION_EVENT
}
