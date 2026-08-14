package io.temporal.samples.kafka.consumer.workflow.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.DataConverter;
import io.temporal.samples.kafka.common.json.TemporalDataConverters;
import io.temporal.samples.kafka.common.kafka.ConsumerRegistry;
import io.temporal.samples.kafka.common.kafka.DeadLetterPublisher;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.common.temporal.ConsumerPattern;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerWorkflowConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  /** Must match the worker's converter — the payload carries {@code Instant}. */
  @Bean
  public DataConverter dataConverter() {
    return TemporalDataConverters.withJavaTime();
  }

  @Bean
  public OrderEmailStarter orderEmailStarter(WorkflowClient client, MeterRegistry meters) {
    return new OrderEmailStarter(client, ConsumerPattern.WORKFLOW, meters);
  }

  /**
   * Holds this JVM's Kafka consumer.
   *
   * <p>A singleton per application, which is the same thing as "per worker" here, because this app
   * runs exactly one worker on exactly one task queue.
   */
  @Bean(destroyMethod = "close")
  public ConsumerRegistry consumerRegistry() {
    return new ConsumerRegistry();
  }

  @Bean(destroyMethod = "close")
  public DeadLetterPublisher deadLetterPublisher() {
    return new DeadLetterPublisher(bootstrapServers);
  }

  @Bean
  public KafkaConsumerSettings kafkaConsumerSettings(ConsumerWorkflowProperties properties) {
    return new KafkaConsumerSettings(
        properties.getInstanceId(),
        bootstrapServers,
        properties.getTopic(),
        properties.getDltTopic(),
        properties.getGroupId(),
        properties.getPollTimeoutMs(),
        properties.getBatchSize());
  }
}
