package io.temporal.samples.kafka.common.config;

/** Topic names and defaults shared by the producer and all three consumers (PRD FR-X5). */
public final class KafkaTopics {

  public static final String ORDERS_COMPLETED = "orders.completed";
  public static final String ORDERS_COMPLETED_DLT = "orders.completed.DLT";

  /**
   * Partition count for the orders topic.
   *
   * <p>This is the throughput ceiling for <em>every</em> consumption pattern in this repo — Kafka
   * assigns each partition to at most one consumer in a group, so adding a 7th consumer of any kind
   * to a 6-partition topic produces an idle consumer, not more throughput. See PRD section 7.4 and
   * {@code scripts/demo-partition-ceiling.sh}. Raise this, not the number of consumers, when you
   * need more parallelism.
   */
  public static final int DEFAULT_PARTITIONS = 6;

  private KafkaTopics() {}
}
