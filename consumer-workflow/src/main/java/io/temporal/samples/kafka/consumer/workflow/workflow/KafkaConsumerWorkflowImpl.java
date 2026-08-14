package io.temporal.samples.kafka.consumer.workflow.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.samples.kafka.common.kafka.PolledBatch;
import io.temporal.samples.kafka.consumer.workflow.activities.KafkaConsumerActivities;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;

/**
 * The loop from the reference architecture, implemented as workflow code.
 *
 * <pre>
 *   subscribe
 *   while not stopped:
 *       poll
 *       start a target workflow per message
 *       dead-letter anything undecodable
 *       commit offsets
 *       continue-as-new if history is getting long
 *   close
 * </pre>
 *
 * <p>Note what is <em>not</em> here: no try/catch around the loop, no retry bookkeeping, no
 * reconnection logic. Activity retry policies cover all of it, and the loop's position is durable —
 * which is the argument for putting the loop in a workflow at all.
 */
public class KafkaConsumerWorkflowImpl implements KafkaConsumerWorkflow {

  private static final Logger log = Workflow.getLogger(KafkaConsumerWorkflowImpl.class);

  /**
   * Hard ceiling on loop iterations per run, as a backstop under {@link
   * io.temporal.workflow.WorkflowInfo#isContinueAsNewSuggested()}.
   *
   * <p>The service's own suggestion is the real trigger; this exists so a demo can force a
   * continue-as-new on command without waiting for thousands of events to accumulate.
   */
  private static final int MAX_ITERATIONS_PER_RUN = 2_000;

  private final KafkaConsumerActivities activities =
      Workflow.newActivityStub(
          KafkaConsumerActivities.class,
          ActivityOptions.newBuilder()
              // Comfortably longer than the Kafka poll timeout carried in the settings.
              .setStartToCloseTimeout(Duration.ofMinutes(2))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setMaximumInterval(Duration.ofSeconds(30))
                      // A broker outage should pause consumption, not fail the consumer.
                      .setMaximumAttempts(0)
                      .build())
              .build());

  private ConsumerParams params;
  private long iterations;
  private boolean stopRequested;

  @Override
  public ConsumerStatus consume(ConsumerParams params) {
    this.params = params;

    activities.subscribe(params.settings());
    log.info(
        "Consumer instance {} started (continuation #{})",
        params.settings().instanceId(),
        params.continuations());

    long processed = params.messagesProcessed();
    long poisoned = params.poisonRecords();

    while (!stopRequested) {
      PolledBatch batch = activities.poll(params.settings());
      iterations++;

      if (!batch.isEmpty()) {
        if (!batch.orders().isEmpty()) {
          activities.startTargetWorkflows(batch.orders());
          processed += batch.orders().size();
        }
        if (!batch.poison().isEmpty()) {
          activities.deadLetter(params.settings(), batch.poison());
          poisoned += batch.poison().size();
        }
        // Only now is it safe to advance the offsets.
        activities.commitOffsets(params.settings(), batch.offsets());
      }

      this.params =
          new ConsumerParams(params.settings(), processed, poisoned, params.continuations());

      if (shouldContinueAsNew()) {
        // Deliberately no close() here. The consumer is worker-local and the next run lands on the
        // same task queue and worker, so keeping it open avoids a needless consumer-group rebalance
        // on every continuation.
        log.info(
            "Continuing as new after {} iterations ({} events in history)",
            iterations,
            Workflow.getInfo().getHistoryLength());
        KafkaConsumerWorkflow next = Workflow.newContinueAsNewStub(KafkaConsumerWorkflow.class);
        return next.consume(
            new ConsumerParams(
                params.settings(), processed, poisoned, params.continuations() + 1));
      }
    }

    activities.close(params.settings());
    log.info("Consumer instance {} stopped after {} messages", params.settings().instanceId(), processed);
    return status();
  }

  private boolean shouldContinueAsNew() {
    // Let the service decide when history is getting large, rather than hardcoding a threshold that
    // would go stale as limits change.
    return Workflow.getInfo().isContinueAsNewSuggested() || iterations >= MAX_ITERATIONS_PER_RUN;
  }

  @Override
  public void stop() {
    log.info("Stop signal received for instance {}", params.settings().instanceId());
    stopRequested = true;
  }

  @Override
  public ConsumerStatus status() {
    return new ConsumerStatus(
        params.settings().instanceId(),
        params.messagesProcessed(),
        params.poisonRecords(),
        iterations,
        params.continuations(),
        Workflow.getInfo().getHistoryLength(),
        stopRequested);
  }
}
