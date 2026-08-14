package io.temporal.samples.kafka.consumer.workflow.bootstrap;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.samples.kafka.common.config.TaskQueues;
import io.temporal.samples.kafka.common.kafka.KafkaConsumerSettings;
import io.temporal.samples.kafka.consumer.workflow.config.ConsumerWorkflowProperties;
import io.temporal.samples.kafka.consumer.workflow.workflow.KafkaConsumerWorkflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Starts this instance's consumer workflow once the worker is ready to serve its task queue.
 *
 * <p>Idempotent: restarting the application reattaches to the workflow that is already running
 * rather than starting a second one. That is the point of a durable consumer loop — the process can
 * come and go, the loop's position cannot.
 */
@Component
public class ConsumerBootstrap {

  private static final Logger log = LoggerFactory.getLogger(ConsumerBootstrap.class);

  private final WorkflowClient client;
  private final KafkaConsumerSettings settings;
  private final ConsumerWorkflowProperties properties;

  public ConsumerBootstrap(
      WorkflowClient client,
      KafkaConsumerSettings settings,
      ConsumerWorkflowProperties properties) {
    this.client = client;
    this.settings = settings;
    this.properties = properties;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void startConsumer() {
    String taskQueue = TaskQueues.kafkaConsumerInstance(properties.getInstanceId());
    String workflowId = taskQueue; // one workflow per instance, named after its queue

    KafkaConsumerWorkflow workflow =
        client.newWorkflowStub(
            KafkaConsumerWorkflow.class,
            WorkflowOptions.newBuilder()
                // Same queue as the activities, served by this process alone.
                .setTaskQueue(taskQueue)
                .setWorkflowId(workflowId)
                .build());

    try {
      var execution =
          WorkflowClient.start(
              workflow::consume, KafkaConsumerWorkflow.ConsumerParams.initial(settings));
      log.info(
          "Started consumer workflow {} on task queue {} (runId={})",
          workflowId,
          taskQueue,
          execution.getRunId());
    } catch (WorkflowExecutionAlreadyStarted alreadyRunning) {
      log.info(
          "Consumer workflow {} is already running — attaching to it rather than starting a second",
          workflowId);
    }
  }
}
