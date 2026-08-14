package io.temporal.samples.kafka.common.workflow;

import java.math.BigDecimal;

public record OrderDetails(
    String orderId, String customerName, String customerEmail, BigDecimal total, int lineCount) {}
