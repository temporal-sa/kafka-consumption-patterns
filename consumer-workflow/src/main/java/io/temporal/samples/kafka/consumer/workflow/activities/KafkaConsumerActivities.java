package io.temporal.samples.kafka.consumer.workflow.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.samples.kafka.common.kafka.ConsumedOrder;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.common.kafka.PoisonRecord;
import io.temporal.samples.kafka.common.kafka.PolledBatch;
import java.util.List;
import java.util.Map;

/**
 * The four steps of the consumer loop, as activities.
 *
 * <p>Each is a separate activity because each becomes a separate pair of events in workflow history
 * — that is the visibility this pattern is bought for, and also its 3-Actions-per-message cost.
 *
 * <p>All of these touch the worker-local Kafka consumer, so they must execute in the JVM that holds
 * it. That is guaranteed structurally: the workflow and these activities share one instance-scoped
 * task queue served by exactly one worker.
 */
@ActivityInterface
public interface KafkaConsumerActivities {

  /** Creates and subscribes the consumer if this worker does not already hold one. */
  void subscribe(KafkaConsumerSettings settings);

  /** One poll. Returns decoded orders, poison records, and the offsets they occupy. */
  PolledBatch poll(KafkaConsumerSettings settings);

  /**
   * Starts a target workflow per order.
   *
   * <p>Idempotent: redelivered orders resolve to the same workflow ID and are counted as duplicates
   * rather than started twice.
   *
   * @return how many were newly started, as opposed to recognized as duplicates
   */
  int startTargetWorkflows(List<ConsumedOrder> orders);

  /** Routes undecodable records to the dead-letter topic so they never block their partition. */
  void deadLetter(KafkaConsumerSettings settings, List<PoisonRecord> poison);

  /**
   * Commits offsets — called only after the batch has been fully handed off.
   *
   * <p>This ordering is the correctness story: a failure anywhere earlier leaves the offset
   * uncommitted, so the records are redelivered rather than lost.
   */
  void commitOffsets(KafkaConsumerSettings settings, Map<String, Long> offsets);

  /** Closes the consumer and leaves the group. Called on shutdown, never before continue-as-new. */
  void close(KafkaConsumerSettings settings);
}
