package io.temporal.samples.kafka.consumer.external.listener;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.temporal.ConsumerPattern;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The entire pattern, in one method.
 *
 * <p>Read a message, start a workflow, acknowledge. That is the whole of the external-application
 * approach — everything else in this module is configuration around it.
 */
@Component
public class OrderCompletedListener {

  private static final Logger log = LoggerFactory.getLogger(OrderCompletedListener.class);

  private final OrderEmailStarter starter;
  private final Counter consumed;

  public OrderCompletedListener(OrderEmailStarter starter, MeterRegistry meters) {
    this.starter = starter;
    this.consumed =
        Counter.builder("kafka.messages.consumed")
            .tag("pattern", ConsumerPattern.EXTERNAL_APP.slug())
            .register(meters);
  }

  @KafkaListener(
      topics = "${app.consumer.topic}",
      groupId = "${app.consumer.group-id}",
      containerFactory = "kafkaListenerContainerFactory")
  public void onOrderCompleted(ConsumerRecord<String, OrderCompleted> record, Acknowledgment ack) {
    OrderCompleted event = record.value();
    consumed.increment();

    // Start first, acknowledge second. If the start throws, the offset is never committed and the
    // error handler retries the same record — so a Temporal outage delays events instead of
    // dropping them. Acknowledging first would create a window where a crash loses the message.
    OrderEmailStarter.StartOutcome outcome = starter.start(event);

    ack.acknowledge();

    if (log.isDebugEnabled()) {
      log.debug(
          "{} order {} (partition={} offset={}) -> {}",
          outcome.duplicate() ? "Skipped duplicate" : "Started workflow for",
          event.orderId(),
          record.partition(),
          record.offset(),
          outcome.workflowId());
    }
  }
}
