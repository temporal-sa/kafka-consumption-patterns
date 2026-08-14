package io.temporal.samples.kafka.common.model;

public record Address(
    String line1, String line2, String city, String region, String postalCode, String country) {}
