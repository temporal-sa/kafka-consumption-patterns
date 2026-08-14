package io.temporal.samples.kafka.worker.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.DataConverter;
import io.temporal.samples.kafka.common.json.TemporalDataConverters;
import io.temporal.samples.kafka.common.temporal.ConsumerPattern;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TemporalConfig {

  /**
   * Picked up automatically by the Temporal Spring Boot starter.
   *
   * <p>Needed because the payloads carry {@code Instant}/{@code LocalDate}. Every module that starts
   * or executes these workflows must declare the same bean — if the starter side and the worker side
   * disagree on Jackson configuration, arguments fail to deserialize at execution time rather than
   * at start time, which is a confusing place to discover it.
   */
  @Bean
  public DataConverter dataConverter() {
    return TemporalDataConverters.withJavaTime();
  }

  /**
   * The same starter the consumers use, so the demo endpoint exercises identical start semantics.
   *
   * <p>Worth keeping shared rather than hand-rolling {@code WorkflowOptions} here: if the demo
   * deduplicated differently from the consumers, it would be useless for diagnosing them.
   */
  @Bean
  public OrderEmailStarter orderEmailStarter(WorkflowClient client, MeterRegistry meters) {
    return new OrderEmailStarter(client, ConsumerPattern.MANUAL, meters);
  }
}
