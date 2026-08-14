package io.temporal.samples.kafka.consumer.activity;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Pattern 3 — Kafka consumers that live inside long-running Temporal activities.
 *
 * <p>Temporal owns the consumer lifecycle: the consumers are visible, restartable, and supervised
 * without a separate deployment to operate. What you give up is per-message history — a message is
 * consumed and a workflow started entirely inside one activity invocation, so nothing about it lands
 * in the consumer workflow's event history.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ConsumerActivityApplication {

  public static void main(String[] args) {
    SpringApplication.run(ConsumerActivityApplication.class, args);
  }
}
