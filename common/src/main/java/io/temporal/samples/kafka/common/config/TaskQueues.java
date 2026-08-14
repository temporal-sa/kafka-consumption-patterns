package io.temporal.samples.kafka.common.config;

/** Temporal task queue names. */
public final class TaskQueues {

  /**
   * Queue for the shared target workflow. All three consumption patterns start {@code
   * OrderEmailWorkflow} here, executed by the one {@code order-email-worker} (PRD FR-X1), so any
   * observed difference between patterns is attributable to consumption alone.
   *
   * <p>Note this pool scales independently of the Kafka partition count — unlike consumption
   * itself. It is usually where throughput work belongs.
   */
  public static final String ORDER_EMAIL = "order-email";

  /**
   * Task queue for a Pattern 2 (workflow-based) consumer instance.
   *
   * <p>Each instance gets its own queue, with the consumer workflow <em>and</em> its activities
   * pinned to it, served by <strong>exactly one</strong> worker (PRD FR-2.7 through FR-2.9). That
   * co-location is what guarantees every activity reaches the worker-local {@code KafkaConsumer}
   * handle. Adding a second worker to one of these queues reintroduces split-brain: activities
   * could land on a JVM with no consumer. Scale out by adding instances, never workers.
   *
   * <p>Used from M3.
   */
  public static String kafkaConsumerInstance(String instanceId) {
    return "kafka-consumer-" + instanceId;
  }

  private TaskQueues() {}
}
