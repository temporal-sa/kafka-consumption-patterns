package io.temporal.samples.kafka.common.kafka;

import io.temporal.samples.kafka.common.model.OrderCompleted;

/**
 * A successfully decoded record, with the Kafka coordinates that produced it.
 *
 * <p>Carries the coordinates because patterns 2 and 3 commit offsets explicitly, and because seeing
 * partition/offset alongside the order in workflow history is a large part of what Pattern 2's extra
 * cost buys.
 */
public record ConsumedOrder(String topic, int partition, long offset, OrderCompleted event) {}
