package io.temporal.samples.kafka.common.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * One Jackson configuration, used for both Kafka payloads and Temporal payloads.
 *
 * <p>The models carry {@link java.time.Instant} and {@link java.time.LocalDate}, which need {@link
 * JavaTimeModule}. Registering it in exactly one place keeps the bytes on the topic and the bytes in
 * workflow history consistent, and avoids the confusing failure where an event round-trips through
 * Kafka fine but fails to deserialize as a workflow argument.
 */
public final class Json {

  public static ObjectMapper newObjectMapper() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  private Json() {}
}
