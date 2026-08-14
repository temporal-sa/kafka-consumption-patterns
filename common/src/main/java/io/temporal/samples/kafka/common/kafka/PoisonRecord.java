package io.temporal.samples.kafka.common.kafka;

/** A record that could not be decoded, on its way to the dead-letter topic (FR-X4). */
public record PoisonRecord(
    String topic, int partition, long offset, String key, String rawValue, String failureReason) {}
