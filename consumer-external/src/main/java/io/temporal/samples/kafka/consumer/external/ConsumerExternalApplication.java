package io.temporal.samples.kafka.consumer.external;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Pattern 1 — an ordinary Spring Boot Kafka consumer that starts a Temporal Workflow per message.
 *
 * <p>This application is a Temporal <b>client</b>, not a worker. It never executes workflow or
 * activity code; it hands work to Temporal and moves on. {@code order-email-worker} is what actually
 * runs {@code OrderEmailWorkflow}.
 *
 * <p>Of the three patterns this is the least novel, which is the point: it is a normal consumer
 * deployment with one extra call in the listener. Nothing about how you build, ship, or scale it
 * changes.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ConsumerExternalApplication {

  public static void main(String[] args) {
    SpringApplication.run(ConsumerExternalApplication.class, args);
  }
}
