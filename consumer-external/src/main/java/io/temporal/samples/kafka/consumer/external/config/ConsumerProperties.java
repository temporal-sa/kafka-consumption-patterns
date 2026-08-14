package io.temporal.samples.kafka.consumer.external.config;

import io.temporal.samples.kafka.common.config.KafkaTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.consumer")
public class ConsumerProperties {

  private String topic = KafkaTopics.ORDERS_COMPLETED;

  private String dltTopic = KafkaTopics.ORDERS_COMPLETED_DLT;

  /**
   * Consumer group. Each pattern uses its own group so all three can run at once and every one of
   * them sees every event — which is what makes side-by-side comparison possible.
   */
  private String groupId = "temporal-external-app";

  /**
   * Listener threads in this instance.
   *
   * <p>One of two scale-out axes; the other is running more instances. Both are capped by the
   * topic's partition count — {@code concurrency × instances > partitions} just leaves threads idle.
   */
  private int concurrency = 3;

  /**
   * Attempts before a record that cannot be processed is routed to the dead-letter topic.
   *
   * <p>This governs transient failures such as Temporal being unreachable. While retries are in
   * progress the offset does not advance, so the partition blocks and nothing is lost — the tradeoff
   * is availability of that partition against giving up on the record. Deserialization failures
   * ignore this and go straight to the DLT, because retrying them can never succeed.
   */
  private int maxStartAttempts = 10;

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

  public int getConcurrency() {
    return concurrency;
  }

  public void setConcurrency(int concurrency) {
    this.concurrency = concurrency;
  }

  public int getMaxStartAttempts() {
    return maxStartAttempts;
  }

  public void setMaxStartAttempts(int maxStartAttempts) {
    this.maxStartAttempts = maxStartAttempts;
  }
}
