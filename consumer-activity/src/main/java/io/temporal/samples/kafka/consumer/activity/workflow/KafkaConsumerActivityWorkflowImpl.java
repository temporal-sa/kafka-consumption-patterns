package io.temporal.samples.kafka.consumer.activity.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.consumer.activity.activities.KafkaConsumeActivity;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

@WorkflowImpl(taskQueues = KafkaConsumerActivityWorkflowImpl.TASK_QUEUE)
public class KafkaConsumerActivityWorkflowImpl implements KafkaConsumerActivityWorkflow {

  /**
   * A single static task queue is enough for this pattern.
   *
   * <p>Unlike Pattern 2, there is no per-instance queue and no co-location constraint: the consumer
   * handle is created and used inside one activity invocation that never returns, so it never has to
   * be reachable from a second activity in the same JVM.
   */
  public static final String TASK_QUEUE = "kafka-consumer-activity";

  private static final Logger log = Workflow.getLogger(KafkaConsumerActivityWorkflowImpl.class);

  @Override
  public void run(KafkaConsumerSettings settings, int parallelConsumers) {
    KafkaConsumeActivity consume =
        Workflow.newActivityStub(
            KafkaConsumeActivity.class,
            ActivityOptions.newBuilder()
                // The activity never returns under normal operation, so this is not a deadline for
                // "how long the work takes" but an upper bound on the life of one consumer. It must
                // be long; liveness is enforced by the heartbeat timeout below, not by this.
                .setStartToCloseTimeout(Duration.ofDays(365))
                // The real liveness check. If the activity stops heartbeating for this long,
                // Temporal treats it as dead and retries it — which is how a wedged or killed
                // consumer gets replaced automatically.
                .setHeartbeatTimeout(Duration.ofSeconds(30))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofSeconds(1))
                        .setMaximumInterval(Duration.ofSeconds(30))
                        // A broker outage should pause consumption, never end it.
                        .setMaximumAttempts(0)
                        .build())
                .build());

    log.info("Starting {} parallel consumer activities", parallelConsumers);

    // Launch them all asynchronously and wait forever. This is the "run multiple activities in
    // parallel" answer from the trade-off table — and, like every other pattern here, it stops
    // gaining throughput once the consumer count reaches the topic's partition count.
    List<Promise<Void>> consumers = new ArrayList<>(parallelConsumers);
    for (int i = 0; i < parallelConsumers; i++) {
      KafkaConsumerSettings perConsumer =
          new KafkaConsumerSettings(
              settings.instanceId() + "-" + i,
              settings.bootstrapServers(),
              settings.topic(),
              settings.dltTopic(),
              settings.groupId(),
              settings.pollTimeoutMs(),
              settings.maxRecordsPerPoll());
      consumers.add(Async.procedure(consume::consume, perConsumer));
    }

    // Completes only on cancellation, which propagates into each activity's next heartbeat.
    Promise.allOf(consumers).get();
  }
}
