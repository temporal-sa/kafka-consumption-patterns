package io.temporal.samples.kafka.consumer.activity.config;

import io.temporal.samples.kafka.common.config.KafkaTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.consumer")
public class ConsumerActivityProperties {

  private String instanceId = "1";

  private String topic = KafkaTopics.ORDERS_COMPLETED;

  private String dltTopic = KafkaTopics.ORDERS_COMPLETED_DLT;

  /** Own consumer group, so this pattern sees every event independently of the other two. */
  private String groupId = "temporal-activity-consumer";

  private long pollTimeoutMs = 5_000;

  private int maxRecordsPerPoll = 50;

  /**
   * How many consume activities run in parallel from the single consumer workflow.
   *
   * <p>Each is one Kafka consumer in the group, so this is the direct equivalent of running more
   * pods in Pattern 1 — and it hits the same ceiling. Beyond the topic's partition count the extra
   * activities sit idle holding worker slots.
   *
   * <p>Whatever you set here, the worker must have more activity execution slots than this. A
   * long-running activity never releases its slot; see the capacity block in application.yml.
   */
  private int parallelConsumers = 2;

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getDltTopic() {
    return dltTopic;
  }

  public void setDltTopic(String dltTopic) {
    this.dltTopic = dltTopic;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public long getPollTimeoutMs() {
    return pollTimeoutMs;
  }

  public void setPollTimeoutMs(long pollTimeoutMs) {
    this.pollTimeoutMs = pollTimeoutMs;
  }

  public int getMaxRecordsPerPoll() {
    return maxRecordsPerPoll;
  }

  public void setMaxRecordsPerPoll(int maxRecordsPerPoll) {
    this.maxRecordsPerPoll = maxRecordsPerPoll;
  }

  public int getParallelConsumers() {
    return parallelConsumers;
  }

  public void setParallelConsumers(int parallelConsumers) {
    this.parallelConsumers = parallelConsumers;
  }
}
