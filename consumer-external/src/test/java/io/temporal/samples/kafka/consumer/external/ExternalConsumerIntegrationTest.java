package io.temporal.samples.kafka.consumer.external;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.temporal.samples.kafka.common.json.Json;
import io.temporal.samples.kafka.common.model.Address;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.model.OrderLine;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test of Pattern 1 against a real Kafka broker and an embedded Temporal test service.
 *
 * <p>The application under test runs unmodified — same listener, same error handler, same start
 * logic. Only the workflow implementation on the worker side is swapped for a recording stand-in,
 * and Temporal itself is the starter's embedded test server rather than a dev server on 7233. That
 * keeps the test hermetic: no Docker Compose, no credentials, nothing listening on a fixed port.
 *
 * <p>Covers the three claims that matter for this pattern:
 *
 * <ol>
 *   <li>a consumed event starts a workflow;
 *   <li>a redelivered event does not start a second one;
 *   <li>a record that cannot be deserialized reaches the dead-letter topic without blocking its
 *       partition.
 * </ol>
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      // Embedded in-process Temporal service; no external dependency.
      "spring.temporal.test-server.enabled=true",
      // Picks up RecordingOrderEmailWorkflow and stands up a worker for the order-email queue.
      "spring.temporal.workers-auto-discovery.packages=io.temporal.samples.kafka.consumer.external",
      // A failed start should reach the DLT quickly here. Production defaults are far more patient.
      "app.consumer.max-start-attempts=2",
      "app.consumer.concurrency=1"
    })
class ExternalConsumerIntegrationTest {

  private static final String TOPIC = "orders.completed";
  private static final String DLT = "orders.completed.DLT";

  /**
   * Kafka 3.8.1, not 3.9.0 as Compose runs.
   *
   * <p>Testcontainers 1.21.0's {@code KafkaContainer} configures {@code apache/kafka:3.9.0} with an
   * advertised listener of {@code 0.0.0.0}, which that broker rejects on startup ("cannot use the
   * nonroutable meta-address"). 3.8.1 is the newest tag this combination starts cleanly on. Revisit
   * when Testcontainers catches up.
   */
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

  @BeforeAll
  static void startKafka() {
    KAFKA.start();
  }

  @AfterAll
  static void stopKafka() {
    KAFKA.stop();
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Test
  void consumedEventStartsWorkflow() {
    OrderCompleted event = order("ORD-IT-001");
    publish(event.orderId(), toJson(event));

    await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(RecordingOrderEmailWorkflow.EXECUTIONS)
                    .containsEntry(event.orderId(), 1));
  }

  @Test
  void redeliveredEventDoesNotStartASecondWorkflow() {
    OrderCompleted event = order("ORD-IT-002");

    // The same event three times, exactly as Kafka replays uncommitted records after a rebalance.
    publish(event.orderId(), toJson(event));
    publish(event.orderId(), toJson(event));
    publish(event.orderId(), toJson(event));

    await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> assertThat(RecordingOrderEmailWorkflow.EXECUTIONS).containsKey(event.orderId()));

    // Hold the assertion for a few seconds so the other two deliveries have been processed
    // (and suppressed) before we conclude the count is stable at one.
    await()
        .during(5, TimeUnit.SECONDS)
        .atMost(20, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(RecordingOrderEmailWorkflow.EXECUTIONS)
                    .containsEntry(event.orderId(), 1));
  }

  @Test
  void malformedRecordIsRoutedToTheDeadLetterTopic() {
    String poisonKey = "ORD-IT-MALFORMED";
    publish(poisonKey, "{\"orderId\": \"" + poisonKey + "\", this is not valid json");

    // A valid record behind the poison one: if the partition stalled, this never arrives.
    OrderCompleted follower = order("ORD-IT-003");
    publish(follower.orderId(), toJson(follower));

    await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertThat(dltKeys()).contains(poisonKey));

    await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertThat(RecordingOrderEmailWorkflow.EXECUTIONS)
                    .containsKey(follower.orderId()));
  }

  // --- helpers -------------------------------------------------------------

  private void publish(String key, String value) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(TOPIC, key, value));
      producer.flush();
    }
  }

  private List<String> dltKeys() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-assert-" + System.nanoTime());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(DLT));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
      List<String> keys = new ArrayList<>();
      records.records(DLT).forEach(record -> keys.add(record.key()));
      return keys;
    }
  }

  private static String toJson(OrderCompleted event) {
    try {
      return Json.newObjectMapper().writeValueAsString(event);
    } catch (Exception e) {
      throw new IllegalStateException("failed to serialize test event", e);
    }
  }

  private static OrderCompleted order(String orderId) {
    return new OrderCompleted(
        orderId,
        "CUST-1",
        "Test User",
        "test@example.com",
        List.of(new OrderLine("SKU-100", "Skillet", 1, new BigDecimal("48.00"))),
        new BigDecimal("48.00"),
        new Address("1 Main St", null, "Seattle", "WA", "98101", "US"),
        Instant.parse("2026-08-12T15:00:00Z"));
  }
}
