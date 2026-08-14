package io.temporal.samples.kafka.common.model;

import java.math.BigDecimal;

public record OrderLine(String sku, String description, int quantity, BigDecimal unitPrice) {}
