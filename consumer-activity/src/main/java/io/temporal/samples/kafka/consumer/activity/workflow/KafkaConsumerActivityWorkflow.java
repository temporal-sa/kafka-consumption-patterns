package io.temporal.samples.kafka.consumer.activity.workflow;

import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Pattern 3 — the workflow exists only to own the lifecycle of one or more consumer activities.
 *
 * <p>Deliberately trivial. All the work happens inside a long-running activity that loops forever,
 * so nothing per-message reaches workflow history. That is the trade: you give up per-message
 * visibility and gain a consumer whose lifecycle Temporal manages, at 1 Action per message rather
 * than 3.
 */
@WorkflowInterface
public interface KafkaConsumerActivityWorkflow {

  /**
   * @param settings how to reach Kafka; the instance ID is suffixed per parallel consumer
   * @param parallelConsumers how many consume activities to run at once — capped in practice by the
   *     topic's partition count, exactly like adding pods in Pattern 1
   */
  @WorkflowMethod
  void run(KafkaConsumerSettings settings, int parallelConsumers);
}
