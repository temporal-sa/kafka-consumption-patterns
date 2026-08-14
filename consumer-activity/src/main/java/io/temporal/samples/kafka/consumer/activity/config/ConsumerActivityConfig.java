package io.temporal.samples.kafka.consumer.activity.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.DataConverter;
import io.temporal.samples.kafka.common.json.TemporalDataConverters;
import io.temporal.samples.kafka.common.kafka.DeadLetterPublisher;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.common.temporal.ConsumerPattern;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerActivityConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  /** Must match the worker's converter — the payload carries {@code Instant}. */
  @Bean
  public DataConverter dataConverter() {
    return TemporalDataConverters.withJavaTime();
  }

  @Bean
  public OrderEmailStarter orderEmailStarter(WorkflowClient client, MeterRegistry meters) {
    return new OrderEmailStarter(client, ConsumerPattern.LONG_RUNNING_ACTIVITY, meters);
  }

  @Bean(destroyMethod = "close")
  public DeadLetterPublisher deadLetterPublisher() {
    return new DeadLetterPublisher(bootstrapServers);
  }

  /**
   * Base settings; the workflow suffixes the instance ID per parallel consumer.
   *
   * <p>No {@code ConsumerRegistry} bean here, unlike Pattern 2 — each consumer is a local variable
   * inside its own activity invocation and never needs to be reachable from anywhere else.
   */
  @Bean
  public KafkaConsumerSettings kafkaConsumerSettings(ConsumerActivityProperties properties) {
    return new KafkaConsumerSettings(
        properties.getInstanceId(),
        bootstrapServers,
        properties.getTopic(),
        properties.getDltTopic(),
        properties.getGroupId(),
        properties.getPollTimeoutMs(),
        properties.getMaxRecordsPerPoll());
  }
}
