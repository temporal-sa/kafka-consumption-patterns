package io.temporal.samples.kafka.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Worker hosting {@code OrderEmailWorkflow} and its activities.
 *
 * <p>Deliberately a separate application from the three consumers. Consuming from Kafka and
 * executing the resulting workflows are different scaling concerns: this pool is bounded by worker
 * capacity, not by the topic's partition count, which is why it is usually the right place to add
 * throughput.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderEmailWorkerApplication {

  public static void main(String[] args) {
    SpringApplication.run(OrderEmailWorkerApplication.class, args);
  }
}
