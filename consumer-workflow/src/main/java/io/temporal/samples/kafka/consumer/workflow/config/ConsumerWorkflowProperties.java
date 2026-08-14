package io.temporal.samples.kafka.consumer.workflow.config;

import io.temporal.samples.kafka.common.config.KafkaTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.consumer")
public class ConsumerWorkflowProperties {

  /**
   * Identifies this consumer instance.
   *
   * <p>Determines the task queue (`kafka-consumer-{instanceId}`), the workflow ID, and the key under
   * which this JVM holds its Kafka consumer. Scale out by starting more instances with distinct IDs
   * — never by adding workers to an existing instance's queue.
   */
  private String instanceId = "1";

  private String topic = KafkaTopics.ORDERS_COMPLETED;

  private String dltTopic = KafkaTopics.ORDERS_COMPLETED_DLT;

  /** Own consumer group, so this pattern sees every event independently of the other two. */
  private String groupId = "temporal-workflow-consumer";

  /**
   * How long each poll blocks waiting for records.
   *
   * <p>Also the idle cost of this pattern: an empty poll still spends one Action, so a quiet topic
   * costs roughly {@code 60000 / pollTimeoutMs} Actions per minute doing nothing. Raise it to make
   * an idle consumer cheaper, at the cost of latency when traffic resumes.
   */
  private long pollTimeoutMs = 5_000;

  /**
   * Records returned per poll — pinned to {@link #DEFAULT_BATCH_SIZE}.
   *
   * @see #DEFAULT_BATCH_SIZE
   */
  private int batchSize = DEFAULT_BATCH_SIZE;

  /**
   * One message per poll cycle.
   *
   * <p>This is what makes the loop cost exactly <b>3 Actions per message</b> (poll + start + commit),
   * matching the published cost table in the reference architecture. Raising it amortises the loop
   * across the batch — at 50 records per poll the loop costs roughly 0.06 Actions per message, which
   * substantially changes that table's conclusion.
   *
   * <p><b>If you change this value, the 3-Actions-per-message row in the reference architecture doc
   * becomes wrong and must be updated to match.</b> See PRD FR-2.5a.
   */
  public static final int DEFAULT_BATCH_SIZE = 1;

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

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
}
