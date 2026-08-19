package io.temporal.samples.kafka.consumer.activity;

import static org.assertj.core.api.Assertions.assertThat;

import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.consumer.activity.bootstrap.ConsumerBootstrap;
import io.temporal.samples.kafka.consumer.activity.config.ConsumerActivityProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Smoke test: the Pattern 3 application context starts on its shipped configuration.
 *
 * <p>Same purpose as the Pattern 2 test. This module's {@code application.yml} carries the worker
 * capacity block that keeps long-running activities from starving the queue, so there is more here
 * worth failing fast on than placeholders alone.
 *
 * <p>No broker is required. {@code DeadLetterPublisher} builds a {@code KafkaProducer}, whose
 * constructor resolves bootstrap servers without connecting to them.
 */
@SpringBootTest(
    // A random port so the test never collides with an instance already running on 8084.
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    // In-process Temporal service, so no dev server has to be up.
    properties = {"spring.temporal.test-server.enabled=true"})
class ConsumerActivityContextTest {

  /**
   * Replaces the real bootstrap with a no-op.
   *
   * <p>Left alone it would start the consumer workflow on {@code ApplicationReadyEvent}, and this
   * pattern's activities never return, so the test would leave one heartbeating for its whole run.
   */
  @MockitoBean ConsumerBootstrap bootstrap;

  @Autowired KafkaConsumerSettings settings;

  @Autowired ConsumerActivityProperties properties;

  /** Resolved from {@code ${TEMPORAL_ADDRESS:local}} in application.yml. */
  @Value("${spring.temporal.connection.target}")
  String temporalTarget;

  @Value("${spring.temporal.namespace}")
  String temporalNamespace;

  @Value("${spring.temporal.workers[0].capacity.max-concurrent-activity-executors}")
  int activitySlots;

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
    assertThat(settings.groupId()).isEqualTo("temporal-activity-consumer");
    assertThat(settings.pollTimeoutMs()).isEqualTo(5_000L);
    assertThat(settings.maxRecordsPerPoll()).isEqualTo(50);
  }

  /**
   * The trap this pattern sets, asserted.
   *
   * <p>A long-running activity holds its execution slot until it finishes, which it never does. If
   * the parallel consumers ever reach the worker's activity slot count, every other activity on the
   * queue waits forever behind them and the worker looks healthy while doing nothing. The headroom
   * is the whole point, so it is worth a test rather than only a comment in the yml.
   */
  @Test
  void parallelConsumersLeaveHeadroomInTheWorkersActivitySlots() {
    assertThat(properties.getParallelConsumers()).isEqualTo(2);
    assertThat(activitySlots).isGreaterThan(properties.getParallelConsumers());
  }
}
