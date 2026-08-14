package io.temporal.samples.kafka.common.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * The event published to the Kafka topic when an eCommerce order completes.
 *
 * <p>This is the payload every one of the three consumption patterns reads, and the input to
 * {@link io.temporal.samples.kafka.common.workflow.OrderEmailWorkflow}. Keeping it identical across
 * all patterns is what makes them comparable (PRD FR-X1).
 *
 * @param orderId used as the Kafka message key (so all events for an order share a partition) and
 *     as the basis for the deterministic workflow ID that makes redelivery idempotent (PRD FR-X2).
 */
public record OrderCompleted(
    String orderId,
    String customerId,
    String customerName,
    String customerEmail,
    List<OrderLine> lines,
    BigDecimal orderTotal,
    Address shippingAddress,
    Instant completedAt) {}
