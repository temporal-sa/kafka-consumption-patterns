package io.temporal.samples.kafka.producer.config;

import io.temporal.samples.kafka.common.config.KafkaTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.producer")
public class ProducerProperties {

  private String topic = KafkaTopics.ORDERS_COMPLETED;

  /**
   * Partition count used when the topic is created on startup.
   *
   * <p>This is the throughput ceiling every consumption pattern shares — see {@link
   * KafkaTopics#DEFAULT_PARTITIONS}. Change it here to run the partition-ceiling experiment.
   */
  private int partitions = KafkaTopics.DEFAULT_PARTITIONS;

  private short replicationFactor = 1;

  /** Seed for the synthetic order generator, so a run can be reproduced exactly. */
  private long seed = 20260812L;

  /**
   * Namespace applied to generated order IDs.
   *
   * <p>Leave unset (the default) and each producer run derives a fresh token from its start time, so
   * restarting never replays order IDs. Set it — together with a fixed {@link #seed} — to reproduce a
   * run byte for byte.
   *
   * <p>This is not cosmetic. Order ID determines the workflow ID, so replayed IDs are silently
   * deduplicated by every consumer: the run looks like it did nothing. See {@code OrderGenerator}.
   */
  private String orderIdPrefix;

  /** Fraction [0,1] of published events that repeat a previously used orderId. */
  private double duplicateRate = 0.0;

  /** Fraction [0,1] of published events that are deliberately unparseable. */
  private double malformedRate = 0.0;

  /** Default rate for the continuous generator, adjustable at runtime via the REST API. */
  private int defaultRatePerSecond = 10;

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public short getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(short replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public long getSeed() {
    return seed;
  }

  public void setSeed(long seed) {
    this.seed = seed;
  }

  public String getOrderIdPrefix() {
    return orderIdPrefix;
  }

  public void setOrderIdPrefix(String orderIdPrefix) {
    this.orderIdPrefix = orderIdPrefix;
  }

  public double getDuplicateRate() {
    return duplicateRate;
  }

  public void setDuplicateRate(double duplicateRate) {
    this.duplicateRate = duplicateRate;
  }

  public double getMalformedRate() {
    return malformedRate;
  }

  public void setMalformedRate(double malformedRate) {
    this.malformedRate = malformedRate;
  }

  public int getDefaultRatePerSecond() {
    return defaultRatePerSecond;
  }

  public void setDefaultRatePerSecond(int defaultRatePerSecond) {
    this.defaultRatePerSecond = defaultRatePerSecond;
  }
}
