package io.temporal.samples.kafka.consumer.external.config;

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

  /** Must match the worker's converter — the payload carries {@code Instant}. */
  @Bean
  public DataConverter dataConverter() {
    return TemporalDataConverters.withJavaTime();
  }

  @Bean
  public OrderEmailStarter orderEmailStarter(WorkflowClient client, MeterRegistry meters) {
    return new OrderEmailStarter(client, ConsumerPattern.EXTERNAL_APP, meters);
  }
}
