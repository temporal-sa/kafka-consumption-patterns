package io.temporal.samples.kafka.consumer.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Pattern 2 — a Temporal Workflow that consumes Kafka through activities.
 *
 * <p>Unlike Pattern 1, this application <b>is</b> a worker: it hosts the consumer workflow and its
 * activities on one instance-scoped task queue, and starts the workflow on boot.
 *
 * <p>Run exactly one of these per instance ID. Two processes sharing an instance ID would both poll
 * the same task queue, and activities could land on the JVM that does not hold the Kafka consumer.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ConsumerWorkflowApplication {

  public static void main(String[] args) {
    SpringApplication.run(ConsumerWorkflowApplication.class, args);
  }
}
