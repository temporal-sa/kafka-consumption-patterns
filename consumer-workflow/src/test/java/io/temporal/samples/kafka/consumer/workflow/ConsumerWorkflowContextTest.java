package io.temporal.samples.kafka.consumer.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.consumer.workflow.bootstrap.ConsumerBootstrap;
import io.temporal.samples.kafka.consumer.workflow.config.ConsumerWorkflowProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Smoke test: the Pattern 2 application context starts on its shipped configuration.
 *
 * <p>Cheap insurance for {@code application.yml}. A mistyped placeholder, a worker pointed at an
 * activity bean that does not exist, or a property renamed on one side only all fail here rather
 * than at {@code spring-boot:run} time. This module otherwise has no test that loads the file at
 * all.
 *
 * <p>No broker is required. {@code DeadLetterPublisher} builds a {@code KafkaProducer}, whose
 * constructor resolves bootstrap servers without connecting to them.
 */
@SpringBootTest(
    // A random port so the test never collides with an instance already running on 8083.
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    // In-process Temporal service, so no dev server has to be up.
    properties = {"spring.temporal.test-server.enabled=true"})
class ConsumerWorkflowContextTest {

  /**
   * Replaces the real bootstrap with a no-op.
   *
   * <p>{@code @SpringBootTest} publishes {@code ApplicationReadyEvent}, so the genuine bean would
   * start the consumer workflow and leave it polling Kafka for the life of the test. The listener
   * still fires; it just does nothing. What this test verifies is that the context stands up, not
   * that the loop runs.
   */
  @MockitoBean ConsumerBootstrap bootstrap;

  @Autowired KafkaConsumerSettings settings;

  @Autowired ConsumerWorkflowProperties properties;

  /** Resolved from {@code ${TEMPORAL_ADDRESS:local}} in application.yml. */
  @Value("${spring.temporal.connection.target}")
  String temporalTarget;

  @Value("${spring.temporal.namespace}")
  String temporalNamespace;

  @Test
  void connectionPlaceholdersResolveToTheirLocalDefaults() {
    assertThat(temporalTarget).isEqualTo("local");
    assertThat(temporalNamespace).isEqualTo("default");
    assertThat(settings.bootstrapServers()).isEqualTo("localhost:9092");
  }

  @Test
  void consumerSettingsCarryTheShippedDefaults() {
    assertThat(settings.instanceId()).isEqualTo("1");
    assertThat(settings.topic()).isEqualTo("orders.completed");
    assertThat(settings.dltTopic()).isEqualTo("orders.completed.DLT");
    assertThat(settings.groupId()).isEqualTo("temporal-workflow-consumer");
    assertThat(settings.pollTimeoutMs()).isEqualTo(5_000L);
  }

  /**
   * Guards the cost claim rather than the wiring.
   *
   * <p>The README and the reference architecture both quote 3 Actions per message for this pattern,
   * which holds only at one record per poll. If someone raises the default, this fails and points at
   * the documentation that has to change with it.
   */
  @Test
  void batchSizeStaysPinnedAtOneRecordPerPoll() {
    assertThat(properties.getBatchSize()).isEqualTo(ConsumerWorkflowProperties.DEFAULT_BATCH_SIZE);
    assertThat(settings.maxRecordsPerPoll()).isEqualTo(1);
  }
}
