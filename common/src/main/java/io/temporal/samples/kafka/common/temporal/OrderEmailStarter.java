package io.temporal.samples.kafka.common.temporal;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.samples.kafka.common.config.TaskQueues;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.workflow.OrderEmailWorkflow;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts {@link OrderEmailWorkflow} for a consumed event. Shared by all three consumption patterns.
 *
 * <p>Identical start semantics and identical metrics across the three patterns is what makes them
 * comparable — if each module rolled its own, differences in this code would masquerade as
 * differences between the patterns.
 *
 * <h2>Why redelivery is safe</h2>
 *
 * Kafka delivers at least once: any consumer restart or rebalance can replay records whose offsets
 * were not yet committed. Idempotency comes from two things together:
 *
 * <ul>
 *   <li>a <b>deterministic workflow ID</b> derived from the order, so a replayed message maps to the
 *       same execution rather than a new one; and
 *   <li>{@code ALLOW_DUPLICATE_FAILED_ONLY}, so a replay after a <em>successful</em> run is rejected
 *       (the email is not sent twice), while a replay after a failed or terminated run is allowed
 *       through so it can recover.
 * </ul>
 *
 * <p>A replay that arrives while the first run is still in flight hits the default conflict policy
 * and is rejected too. Both rejections surface as {@link WorkflowExecutionAlreadyStarted}, which is
 * caught here and treated as success: the work is already accounted for, so the caller should commit
 * the offset and move on.
 *
 * <p>Catching that exception, rather than using {@code WorkflowIdConflictPolicy.USE_EXISTING}, is a
 * deliberate choice. USE_EXISTING silently attaches to a running execution and returns no indication
 * that anything was deduplicated, so duplicates become unmeasurable. Here every deduplication is
 * counted, which is what lets the duplicate-injection demo actually prove something.
 */
public class OrderEmailStarter {

  private static final Logger log = LoggerFactory.getLogger(OrderEmailStarter.class);

  private final WorkflowClient client;
  private final ConsumerPattern pattern;
  private final Counter started;
  private final Counter duplicates;
  private final Timer eventToStartLatency;

  public OrderEmailStarter(WorkflowClient client, ConsumerPattern pattern, MeterRegistry meters) {
    this.client = client;
    this.pattern = pattern;
    this.started =
        Counter.builder("temporal.workflows.started").tag("pattern", pattern.slug()).register(meters);
    this.duplicates =
        Counter.builder("temporal.workflows.duplicate_skipped")
            .tag("pattern", pattern.slug())
            .register(meters);
    this.eventToStartLatency =
        Timer.builder("kafka.event.to.workflow.start")
            .description("Event timestamp until the workflow start is accepted by Temporal")
            .tag("pattern", pattern.slug())
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(meters);
  }

  /**
   * Starts the workflow, or recognises that this event was already handled.
   *
   * <p>Returns only after Temporal has durably accepted the start. Callers must not commit the Kafka
   * offset until this returns normally — that ordering is what turns at-least-once delivery into
   * "no event is ever dropped".
   *
   * @return the outcome, so callers can log or count it
   */
  public StartOutcome start(OrderCompleted event) {
    String workflowId = WorkflowIds.orderEmail(pattern, event.orderId());

    OrderEmailWorkflow workflow =
        client.newWorkflowStub(
            OrderEmailWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TaskQueues.ORDER_EMAIL)
                .setWorkflowId(workflowId)
                .setWorkflowIdReusePolicy(
                    WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
                .build());

    try {
      // start(), not execute(): the consumer hands the work to Temporal and moves on. Blocking
      // until the email is sent would tie consumption throughput to downstream latency, which is
      // the coupling this whole integration exists to remove.
      var execution = WorkflowClient.start(workflow::sendOrderEmail, event);
      started.increment();
      recordLatency(event);
      return new StartOutcome(workflowId, execution.getRunId(), false);
    } catch (WorkflowExecutionAlreadyStarted alreadyStarted) {
      duplicates.increment();
      log.debug("Order {} already handled — skipping duplicate", event.orderId());
      return new StartOutcome(workflowId, null, true);
    }
  }

  private void recordLatency(OrderCompleted event) {
    if (event.completedAt() != null) {
      Duration latency = Duration.between(event.completedAt(), Instant.now());
      if (!latency.isNegative()) {
        eventToStartLatency.record(latency);
      }
    }
  }

  public record StartOutcome(String workflowId, String runId, boolean duplicate) {}
}
