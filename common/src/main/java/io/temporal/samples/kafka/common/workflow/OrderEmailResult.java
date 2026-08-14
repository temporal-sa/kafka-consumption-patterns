package io.temporal.samples.kafka.common.workflow;

import java.time.Instant;

public record OrderEmailResult(
    String orderId, String emailMessageId, String invoiceRef, Instant sentAt) {}
