package io.temporal.samples.kafka.producer.publish;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.producer.config.ProducerProperties;
import io.temporal.samples.kafka.producer.generate.OrderGenerator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes events, applying the duplicate and malformed injection rates.
 *
 * <p>Those two knobs exist to prove consumer behaviour rather than to be realistic: duplicates
 * demonstrate that deterministic workflow IDs make redelivery harmless, and malformed records
 * demonstrate that a poison message is routed to the DLT instead of stalling its partition.
 */
@Component
public class OrderPublisher {

  private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);

  private final KafkaTemplate<String, OrderCompleted> orderTemplate;
  private final KafkaTemplate<String, String> rawTemplate;
  private final OrderGenerator generator;
  private final ProducerProperties properties;
  private final Counter published;
  private final Counter duplicates;
  private final Counter malformed;

  public OrderPublisher(
      KafkaTemplate<String, OrderCompleted> orderTemplate,
      KafkaTemplate<String, String> rawTemplate,
      OrderGenerator generator,
      ProducerProperties properties,
      MeterRegistry meters) {
    this.orderTemplate = orderTemplate;
    this.rawTemplate = rawTemplate;
    this.generator = generator;
    this.properties = properties;
    this.published = Counter.builder("producer.events.published").register(meters);
    this.duplicates = Counter.builder("producer.events.duplicate").register(meters);
    this.malformed = Counter.builder("producer.events.malformed").register(meters);
  }

  /** Publishes one event, honouring the configured injection rates. Returns what was sent. */
  public PublishOutcome publishNext() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();

    if (properties.getMalformedRate() > 0 && rnd.nextDouble() < properties.getMalformedRate()) {
      return publishMalformed();
    }

    if (properties.getDuplicateRate() > 0 && rnd.nextDouble() < properties.getDuplicateRate()) {
      OrderCompleted repeat = generator.replayRandomPrevious();
      if (repeat != null) {
        duplicates.increment();
        return publish(repeat, true);
      }
    }

    return publish(generator.next(), false);
  }

  public PublishOutcome publish(OrderCompleted event) {
    return publish(event, false);
  }

  private PublishOutcome publish(OrderCompleted event, boolean duplicate) {
    try {
      // Keyed by orderId so every event for an order lands on one partition and is therefore
      // handled by one consumer, in order.
      SendResult<String, OrderCompleted> result =
          orderTemplate.send(properties.getTopic(), event.orderId(), event).get();
      published.increment();
      return new PublishOutcome(
          event.orderId(),
          result.getRecordMetadata().partition(),
          result.getRecordMetadata().offset(),
          duplicate,
          false);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted publishing order " + event.orderId(), e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("failed to publish order " + event.orderId(), e);
    }
  }

  private PublishOutcome publishMalformed() {
    String key = "MALFORMED-" + System.nanoTime();
    String garbage = "{\"orderId\": \"" + key + "\", this is not valid json";
    try {
      SendResult<String, String> result =
          rawTemplate.send(properties.getTopic(), key, garbage).get();
      malformed.increment();
      log.info("Published malformed record with key {}", key);
      return new PublishOutcome(
          key,
          result.getRecordMetadata().partition(),
          result.getRecordMetadata().offset(),
          false,
          true);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted publishing malformed record", e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("failed to publish malformed record", e);
    }
  }

  public record PublishOutcome(
      String orderId, int partition, long offset, boolean duplicate, boolean malformed) {}
}
