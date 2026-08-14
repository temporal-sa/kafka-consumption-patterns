package io.temporal.samples.kafka.common.temporal;

/**
 * Deterministic workflow IDs — the mechanism that makes at-least-once Kafka delivery safe.
 *
 * <p>Kafka redelivers on any consumer restart or rebalance. Because the ID is derived from the
 * order, a redelivered message resolves to the same workflow execution, and starting it with {@code
 * WorkflowIdConflictPolicy.USE_EXISTING} is a no-op rather than an error to catch (PRD FR-X2). This
 * is why the repo does not need Kafka transactions (PRD NG4).
 */
public final class WorkflowIds {

  public static String orderEmail(ConsumerPattern pattern, String orderId) {
    return "order-email-" + pattern.slug() + "-" + orderId;
  }

  private WorkflowIds() {}
}
