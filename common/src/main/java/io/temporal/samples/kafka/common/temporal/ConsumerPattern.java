package io.temporal.samples.kafka.common.temporal;

/**
 * Which consumption pattern started a given workflow.
 *
 * <p>The slug becomes part of the workflow ID (see {@link WorkflowIds}) so that all three patterns
 * can run simultaneously against the same topic — each in its own consumer group — without their
 * idempotency keys colliding. That side-by-side run is the whole point of the repo.
 */
public enum ConsumerPattern {

  /** Pattern 1 — plain Spring Boot app with a {@code @KafkaListener}. */
  EXTERNAL_APP("ext"),

  /** Pattern 2 — a Temporal workflow polling via activities. */
  WORKFLOW("wf"),

  /** Pattern 3 — a single long-running, heartbeating activity. */
  LONG_RUNNING_ACTIVITY("act"),

  /** Started by hand (demo endpoint), not by a consumer. */
  MANUAL("manual");

  private final String slug;

  ConsumerPattern(String slug) {
    this.slug = slug;
  }

  public String slug() {
    return slug;
  }
}
