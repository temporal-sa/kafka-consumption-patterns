package io.temporal.samples.kafka.common.workflow;

import java.time.LocalDate;

public record ShippingDetails(
    String orderId, String carrier, String trackingNumber, LocalDate estimatedDelivery) {}
