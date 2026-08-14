package io.temporal.samples.kafka.consumer.workflow.workflow;

import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 2 — the consumer loop <em>is</em> a workflow.
 *
 * <p>Polls Kafka, starts a target workflow per message, and commits offsets, all through activities,
 * so every message consumed appears in this workflow's event history. That visibility is the entire
 * reason to choose this pattern, and it is what the extra Action cost pays for.
 *
 * <p>Because history is finite, the loop continues-as-new when the service suggests it.
 */
@WorkflowInterface
public interface KafkaConsumerWorkflow {

  @WorkflowMethod
  ConsumerStatus consume(ConsumerParams params);

  /** Asks the loop to finish its current iteration, close the consumer, and complete. */
  @SignalMethod
  void stop();

  /** Progress without touching Kafka — useful while a run is in flight. */
  @QueryMethod
  ConsumerStatus status();

  /**
   * Loop state carried across continue-as-new.
   *
   * @param settings how to reach Kafka; {@code instanceId} also keys the worker-local consumer
   * @param messagesProcessed cumulative across all continued runs, not just this one
   * @param continuations how many times the loop has continued-as-new
   */
  record ConsumerParams(
      KafkaConsumerSettings settings, long messagesProcessed, long poisonRecords, int continuations) {

    public static ConsumerParams initial(KafkaConsumerSettings settings) {
      return new ConsumerParams(settings, 0, 0, 0);
    }
  }
}
