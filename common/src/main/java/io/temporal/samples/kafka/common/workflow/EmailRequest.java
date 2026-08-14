package io.temporal.samples.kafka.common.workflow;

public record EmailRequest(
    String orderId,
    String toAddress,
    String toName,
    String subject,
    String body,
    String invoiceRef) {}
