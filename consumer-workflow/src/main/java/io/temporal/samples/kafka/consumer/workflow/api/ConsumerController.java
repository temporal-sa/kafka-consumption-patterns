package io.temporal.samples.kafka.consumer.workflow.api;

import io.temporal.client.WorkflowClient;
import io.temporal.samples.kafka.common.config.TaskQueues;
import io.temporal.samples.kafka.consumer.workflow.config.ConsumerWorkflowProperties;
import io.temporal.samples.kafka.consumer.workflow.workflow.ConsumerStatus;
import io.temporal.samples.kafka.consumer.workflow.workflow.KafkaConsumerWorkflow;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Inspect and stop this instance's consumer loop without going through the Temporal CLI. */
@RestController
@RequestMapping("/consumer")
public class ConsumerController {

  private final WorkflowClient client;
  private final ConsumerWorkflowProperties properties;

  public ConsumerController(WorkflowClient client, ConsumerWorkflowProperties properties) {
    this.client = client;
    this.properties = properties;
  }

  /** Queries the running workflow — no Kafka involved, no effect on the loop. */
  @GetMapping
  public ConsumerStatus status() {
    return stub().status();
  }

  /**
   * Signals the loop to finish its current iteration, close the consumer, and complete.
   *
   * <p>Graceful: the consumer leaves its group cleanly rather than waiting to be timed out, so the
   * remaining instances rebalance immediately.
   */
  @DeleteMapping
  public Map<String, Object> stop() {
    stub().stop();
    return Map.of(
        "instanceId", properties.getInstanceId(),
        "stopRequested", true);
  }

  private KafkaConsumerWorkflow stub() {
    return client.newWorkflowStub(
        KafkaConsumerWorkflow.class,
        TaskQueues.kafkaConsumerInstance(properties.getInstanceId()));
  }
}
