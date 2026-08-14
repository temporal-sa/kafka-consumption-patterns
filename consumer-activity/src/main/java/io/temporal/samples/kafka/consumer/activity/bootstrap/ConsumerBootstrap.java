package io.temporal.samples.kafka.consumer.activity.bootstrap;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.consumer.activity.config.ConsumerActivityProperties;
import io.temporal.samples.kafka.consumer.activity.workflow.KafkaConsumerActivityWorkflow;
import io.temporal.samples.kafka.consumer.activity.workflow.KafkaConsumerActivityWorkflowImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Starts the consumer workflow once the worker is ready.
 *
 * <p>Idempotent, so restarting the process reattaches rather than starting a second consumer
 * workflow. If the process is killed outright, the activity stops heartbeating and Temporal retries
 * it on whichever worker is available — the consumer comes back without anyone restarting anything.
 */
@Component
public class ConsumerBootstrap {

  private static final Logger log = LoggerFactory.getLogger(ConsumerBootstrap.class);

  private final WorkflowClient client;
  private final KafkaConsumerSettings settings;
  private final ConsumerActivityProperties properties;

  public ConsumerBootstrap(
      WorkflowClient client,
      KafkaConsumerSettings settings,
      ConsumerActivityProperties properties) {
    this.client = client;
    this.settings = settings;
    this.properties = properties;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void startConsumer() {
    String workflowId = "kafka-consumer-activity-" + properties.getInstanceId();

    KafkaConsumerActivityWorkflow workflow =
        client.newWorkflowStub(
            KafkaConsumerActivityWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(KafkaConsumerActivityWorkflowImpl.TASK_QUEUE)
                .setWorkflowId(workflowId)
                .build());

    try {
      var execution =
          WorkflowClient.start(workflow::run, settings, properties.getParallelConsumers());
      log.info(
          "Started consumer workflow {} with {} parallel consumers (runId={})",
          workflowId,
          properties.getParallelConsumers(),
          execution.getRunId());
    } catch (WorkflowExecutionAlreadyStarted alreadyRunning) {
      log.info("Consumer workflow {} is already running — attaching to it", workflowId);
    }
  }
}
