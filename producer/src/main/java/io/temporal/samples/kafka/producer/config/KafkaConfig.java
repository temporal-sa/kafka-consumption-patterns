package io.temporal.samples.kafka.producer.config;

import io.temporal.samples.kafka.common.config.KafkaTopics;
import io.temporal.samples.kafka.common.json.Json;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  /** Created on startup so the quickstart needs no manual topic administration (PRD FR-P7). */
  @Bean
  public NewTopic ordersTopic(ProducerProperties properties) {
    return TopicBuilder.name(properties.getTopic())
        .partitions(properties.getPartitions())
        .replicas(properties.getReplicationFactor())
        .build();
  }

  /** Dead-letter topic, created up front so consumers (M2 onward) can publish to it immediately. */
  @Bean
  public NewTopic ordersDltTopic(ProducerProperties properties) {
    return TopicBuilder.name(KafkaTopics.ORDERS_COMPLETED_DLT)
        .partitions(properties.getPartitions())
        .replicas(properties.getReplicationFactor())
        .build();
  }

  @Bean
  public KafkaTemplate<String, OrderCompleted> orderTemplate() {
    // Shares the common ObjectMapper so Instant is serialized the same way here as it is in
    // Temporal payloads — otherwise an event round-trips through Kafka but fails as a workflow
    // argument.
    JsonSerializer<OrderCompleted> valueSerializer = new JsonSerializer<>(Json.newObjectMapper());
    valueSerializer.setAddTypeInfo(false);
    return new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(baseProducerConfig(), new StringSerializer(), valueSerializer));
  }

  /**
   * Separate template used only to publish deliberately malformed records (PRD FR-P6), which by
   * definition cannot go through the typed serializer.
   */
  @Bean
  public KafkaTemplate<String, String> rawTemplate() {
    ProducerFactory<String, String> factory =
        new DefaultKafkaProducerFactory<>(
            baseProducerConfig(), new StringSerializer(), new StringSerializer());
    return new KafkaTemplate<>(factory);
  }

  private Map<String, Object> baseProducerConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    return config;
  }
}
