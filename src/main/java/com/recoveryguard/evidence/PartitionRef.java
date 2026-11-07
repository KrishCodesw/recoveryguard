package com.recoveryguard.evidence;

public record PartitionRef(String topic, int partition) {

    public PartitionRef {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be blank");
        }

        if (partition < 0) {
            throw new IllegalArgumentException("partition cannot be negative");
        }
    }
}
