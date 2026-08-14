package io.temporal.samples.kafka.consumer.external.config;

import io.temporal.samples.kafka.common.json.Json;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class KafkaConsumerConfig {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Bean
  public ConsumerFactory<String, OrderCompleted> consumerFactory(ConsumerProperties properties) {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Offsets are committed explicitly, after Temporal has accepted the start. Auto-commit would
    // advance offsets on a timer regardless of whether the work was handed off, which is how
    // events get silently dropped.
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    // ErrorHandlingDeserializer turns a deserialization failure into a record the listener
    // container can route, instead of an exception thrown deep in the poll loop that would
    // otherwise retry the same poison record forever and wedge the partition.
    JsonDeserializer<OrderCompleted> delegate =
        new JsonDeserializer<>(OrderCompleted.class, Json.newObjectMapper(), false);
    delegate.addTrustedPackages("io.temporal.samples.kafka.common.model");

    return new DefaultKafkaConsumerFactory<>(
        config, new StringDeserializer(), new ErrorHandlingDeserializer<>(delegate));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, OrderCompleted> kafkaListenerContainerFactory(
      ConsumerFactory<String, OrderCompleted> consumerFactory,
      ConsumerProperties properties,
      DefaultErrorHandler errorHandler) {

    ConcurrentKafkaListenerContainerFactory<String, OrderCompleted> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setConcurrency(properties.getConcurrency());
    factory.setCommonErrorHandler(errorHandler);
    // MANUAL_IMMEDIATE: the listener decides exactly when an offset is committed, and commits it
    // as soon as it decides. See OrderCompletedListener for why the ordering matters.
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  /** Byte-level template so poison records reach the DLT unchanged, exactly as they arrived. */
  @Bean
  public KafkaTemplate<Object, Object> dltTemplate() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
  }

  @Bean
  public DefaultErrorHandler errorHandler(
      KafkaTemplate<Object, Object> dltTemplate, ConsumerProperties properties) {

    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            dltTemplate,
            (record, exception) -> {
              log.error(
                  "Routing record to {} — topic={} partition={} offset={} reason={}",
                  properties.getDltTopic(),
                  record.topic(),
                  record.partition(),
                  record.offset(),
                  exception.getMessage());
              // Same partition on the DLT, so ordering within a key is preserved there too.
              return new org.apache.kafka.common.TopicPartition(
                  properties.getDltTopic(), record.partition());
            });

    ExponentialBackOffWithMaxRetries backOff =
        new ExponentialBackOffWithMaxRetries(properties.getMaxStartAttempts());
    backOff.setInitialInterval(1_000L);
    backOff.setMultiplier(2.0);
    backOff.setMaxInterval(30_000L);

    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
    // A record that cannot be deserialized will never deserialize on retry. Send it straight to
    // the DLT rather than burning the backoff schedule on it while its partition stalls.
    handler.addNotRetryableExceptions(
        org.springframework.kafka.support.serializer.DeserializationException.class);
    return handler;
  }
}
