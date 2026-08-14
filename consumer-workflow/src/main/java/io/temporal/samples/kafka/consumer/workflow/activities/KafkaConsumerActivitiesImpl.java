package io.temporal.samples.kafka.consumer.workflow.activities;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.samples.kafka.common.kafka.ConsumedOrder;
import io.temporal.samples.kafka.common.kafka.ConsumerRegistry;
import io.temporal.samples.kafka.common.kafka.DeadLetterPublisher;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.common.kafka.KafkaConsumers;
import io.temporal.samples.kafka.common.kafka.PoisonRecord;
import io.temporal.samples.kafka.common.kafka.PolledBatch;
import io.temporal.samples.kafka.common.temporal.ConsumerPattern;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Where this pattern actually touches Kafka.
 *
 * <p>Every method here reaches into the worker-local {@link ConsumerRegistry}, so all of them must
 * run in the same JVM. See the module README for why that is a structural guarantee rather than a
 * hope.
 */
@Component("kafkaConsumerActivities")
public class KafkaConsumerActivitiesImpl implements KafkaConsumerActivities {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerActivitiesImpl.class);

  private final ConsumerRegistry registry;
  private final DeadLetterPublisher deadLetterPublisher;
  private final OrderEmailStarter starter;
  private final Counter consumed;
  private final Counter deadLettered;

  public KafkaConsumerActivitiesImpl(
      ConsumerRegistry registry,
      DeadLetterPublisher deadLetterPublisher,
      OrderEmailStarter starter,
      MeterRegistry meters) {
    this.registry = registry;
    this.deadLetterPublisher = deadLetterPublisher;
    this.starter = starter;
    this.consumed =
        Counter.builder("kafka.messages.consumed")
            .tag("pattern", ConsumerPattern.WORKFLOW.slug())
            .register(meters);
    this.deadLettered =
        Counter.builder("kafka.records.dlt")
            .tag("pattern", ConsumerPattern.WORKFLOW.slug())
            .register(meters);
  }

  @Override
  public void subscribe(KafkaConsumerSettings settings) {
    registry.get(settings);
  }

  @Override
  public PolledBatch poll(KafkaConsumerSettings settings) {
    PolledBatch batch =
        KafkaConsumers.poll(
            registry.get(settings), Duration.ofMillis(settings.pollTimeoutMs()));
    consumed.increment(batch.orders().size());
    return batch;
  }

  @Override
  public int startTargetWorkflows(List<ConsumedOrder> orders) {
    int started = 0;
    for (ConsumedOrder order : orders) {
      if (!starter.start(order.event()).duplicate()) {
        started++;
      }
    }
    return started;
  }

  @Override
  public void deadLetter(KafkaConsumerSettings settings, List<PoisonRecord> poison) {
    deadLetterPublisher.publish(settings.dltTopic(), poison);
    deadLettered.increment(poison.size());
  }

  @Override
  public void commitOffsets(KafkaConsumerSettings settings, Map<String, Long> offsets) {
    KafkaConsumers.commit(registry.get(settings), offsets);
  }

  @Override
  public void close(KafkaConsumerSettings settings) {
    registry.close(settings.instanceId());
    log.info("Closed consumer for instance {}", settings.instanceId());
  }
}
