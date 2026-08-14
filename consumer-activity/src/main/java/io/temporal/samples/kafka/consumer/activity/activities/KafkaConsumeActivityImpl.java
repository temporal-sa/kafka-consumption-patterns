package io.temporal.samples.kafka.consumer.activity.activities;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.activity.Activity;
import io.temporal.client.ActivityCompletionException;
import io.temporal.samples.kafka.common.kafka.ConsumedOrder;
import io.temporal.samples.kafka.common.kafka.DeadLetterPublisher;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.common.kafka.KafkaConsumers;
import io.temporal.samples.kafka.common.kafka.PolledBatch;
import io.temporal.samples.kafka.common.temporal.ConsumerPattern;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import io.temporal.samples.kafka.consumer.activity.workflow.KafkaConsumerActivityWorkflowImpl;
import io.temporal.spring.boot.ActivityImpl;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * One Kafka consumer, living entirely inside one activity invocation that never returns.
 *
 * <p>The consumer is a local variable rather than a worker-scoped registry entry: it is created,
 * used, and closed within this method, so nothing else ever needs to reach it. That is why this
 * pattern needs neither the instance-scoped task queue nor the single-worker rule that Pattern 2
 * does — a simpler deployment story for less visibility.
 */
@Component("kafkaConsumeActivity")
@ActivityImpl(taskQueues = KafkaConsumerActivityWorkflowImpl.TASK_QUEUE)
public class KafkaConsumeActivityImpl implements KafkaConsumeActivity {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumeActivityImpl.class);

  private final OrderEmailStarter starter;
  private final DeadLetterPublisher deadLetterPublisher;
  private final Counter consumed;
  private final Counter deadLettered;

  public KafkaConsumeActivityImpl(
      OrderEmailStarter starter, DeadLetterPublisher deadLetterPublisher, MeterRegistry meters) {
    this.starter = starter;
    this.deadLetterPublisher = deadLetterPublisher;
    this.consumed =
        Counter.builder("kafka.messages.consumed")
            .tag("pattern", ConsumerPattern.LONG_RUNNING_ACTIVITY.slug())
            .register(meters);
    this.deadLettered =
        Counter.builder("kafka.records.dlt")
            .tag("pattern", ConsumerPattern.LONG_RUNNING_ACTIVITY.slug())
            .register(meters);
  }

  @Override
  public void consume(KafkaConsumerSettings settings) {
    log.info(
        "Consumer activity {} starting (attempt {})",
        settings.instanceId(),
        Activity.getExecutionContext().getInfo().getAttempt());

    long processed = 0;

    // try-with-resources: the consumer is closed on cancellation, on error, and on worker
    // shutdown. Without this the consumer group would wait out the session timeout on every
    // restart before rebalancing.
    try (KafkaConsumer<String, byte[]> consumer = KafkaConsumers.create(settings)) {
      consumer.subscribe(List.of(settings.topic()));

      // Heartbeat before the first poll so Temporal sees the activity as alive immediately,
      // rather than only after the first record arrives on a quiet topic.
      heartbeat(settings, processed, Map.of());

      while (true) {
        PolledBatch batch =
            KafkaConsumers.poll(consumer, Duration.ofMillis(settings.pollTimeoutMs()));

        if (!batch.orders().isEmpty()) {
          for (ConsumedOrder order : batch.orders()) {
            starter.start(order.event());
          }
          consumed.increment(batch.orders().size());
          processed += batch.orders().size();
        }

        if (!batch.poison().isEmpty()) {
          deadLetterPublisher.publish(settings.dltTopic(), batch.poison());
          deadLettered.increment(batch.poison().size());
        }

        if (!batch.isEmpty()) {
          // Commit only after every record in the batch has been handed off or dead-lettered.
          KafkaConsumers.commit(consumer, batch.offsets());
        }

        // Every iteration, including idle ones. The SDK throttles these to a fraction of the
        // heartbeat timeout, so calling it this often costs far fewer Actions than it appears to —
        // do not hand-roll your own throttling on top.
        heartbeat(settings, processed, batch.offsets());
      }
    } catch (ActivityCompletionException cancelled) {
      // Thrown by heartbeat() when the workflow is cancelled or the activity is no longer
      // considered current. The only clean way out of an infinite loop.
      log.info(
          "Consumer activity {} cancelled after {} messages — closing consumer",
          settings.instanceId(),
          processed);
      throw cancelled;
    }
  }

  /**
   * Reports liveness and progress.
   *
   * <p>The details are visible in the Web UI on the pending activity, which is the "some visibility"
   * this pattern offers. They are not load-bearing for correctness: recovery after a failure comes
   * from Kafka's committed offsets, not from these.
   */
  private void heartbeat(KafkaConsumerSettings settings, long processed, Map<String, Long> offsets) {
    Activity.getExecutionContext()
        .heartbeat(new ConsumerProgress(settings.instanceId(), processed, offsets));
  }

  /** Heartbeat details: what this consumer has done and where it is. */
  public record ConsumerProgress(
      String instanceId, long messagesProcessed, Map<String, Long> committedOffsets) {}
}
