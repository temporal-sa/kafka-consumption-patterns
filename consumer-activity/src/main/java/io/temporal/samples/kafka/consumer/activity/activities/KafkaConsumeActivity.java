package io.temporal.samples.kafka.consumer.activity.activities;

import io.temporal.activity.ActivityInterface;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;

@ActivityInterface
public interface KafkaConsumeActivity {

  /**
   * Consumes forever: poll, start a target workflow per message, commit, heartbeat, repeat.
   *
   * <p>Returns only when the workflow is cancelled. Everything about one consumer's life happens
   * inside this single invocation, which is why nothing per-message appears in workflow history.
   */
  void consume(KafkaConsumerSettings settings);
}
