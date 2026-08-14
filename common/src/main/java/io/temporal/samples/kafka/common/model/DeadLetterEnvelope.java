package io.temporal.samples.kafka.common.model;

import java.time.Instant;

/**
 * Wrapper published to {@code orders.completed.DLT} when a record cannot be handled.
 *
 * <p>All three patterns must behave the same way here (PRD FR-X4): a poison record is routed aside
 * and its offset committed, so it never stalls its partition. Used from M2.
 */
public record DeadLetterEnvelope(
    String sourceTopic,
    int partition,
    long offset,
    String messageKey,
    String rawPayload,
    String failureReason,
    String consumerPattern,
    Instant deadLetteredAt) {}
