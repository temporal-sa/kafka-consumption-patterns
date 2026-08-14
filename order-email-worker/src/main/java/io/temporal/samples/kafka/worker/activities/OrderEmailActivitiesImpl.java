package io.temporal.samples.kafka.worker.activities;

import io.temporal.activity.Activity;
import io.temporal.samples.kafka.common.config.TaskQueues;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.workflow.EmailRequest;
import io.temporal.samples.kafka.common.workflow.OrderDetails;
import io.temporal.samples.kafka.common.workflow.OrderEmailActivities;
import io.temporal.samples.kafka.common.workflow.ShippingDetails;
import io.temporal.samples.kafka.worker.chaos.ChaosController;
import io.temporal.samples.kafka.worker.chaos.ChaosInjector;
import io.temporal.spring.boot.ActivityImpl;
import java.time.LocalDate;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stand-ins for Customer ABC's order database, shipping system, invoice renderer, and email
 * provider. Each one runs the chaos injector first so it can be made slow, flaky, or dead at
 * runtime.
 */
@Component
@ActivityImpl(taskQueues = TaskQueues.ORDER_EMAIL)
public class OrderEmailActivitiesImpl implements OrderEmailActivities {

  private static final Logger log = LoggerFactory.getLogger(OrderEmailActivitiesImpl.class);

  private final ChaosInjector chaos;

  public OrderEmailActivitiesImpl(ChaosInjector chaos) {
    this.chaos = chaos;
  }

  @Override
  public OrderDetails lookupOrder(String orderId) {
    logAttempt("lookupOrder", orderId);
    chaos.apply(ChaosController.ORDER_DB);
    // A real implementation would read the order here; the event payload stands in for it.
    return new OrderDetails(
        orderId,
        "Customer " + orderId.substring(Math.max(0, orderId.length() - 4)),
        "customer+" + orderId + "@example.com",
        java.math.BigDecimal.ZERO,
        0);
  }

  @Override
  public ShippingDetails lookupShippingDetails(String orderId) {
    logAttempt("lookupShippingDetails", orderId);
    chaos.apply(ChaosController.SHIPPING_DB);
    return new ShippingDetails(
        orderId, "ACME Freight", "TRK-" + orderId, LocalDate.now().plusDays(4));
  }

  @Override
  public String generateInvoice(OrderCompleted event, OrderDetails order) {
    logAttempt("generateInvoice", event.orderId());
    chaos.apply(ChaosController.INVOICE);
    return "invoice://" + event.orderId() + "/" + UUID.randomUUID();
  }

  @Override
  public String sendEmail(EmailRequest request) {
    logAttempt("sendEmail", request.orderId());
    chaos.apply(ChaosController.EMAIL);
    String messageId = "msg-" + UUID.randomUUID();
    log.info("Email sent to {} for order {} ({})", request.toAddress(), request.orderId(), messageId);
    return messageId;
  }

  /**
   * Logs the attempt number so retries are obvious in the console during a chaos demo — the Web UI
   * shows the same thing in the pending-activity panel.
   */
  private void logAttempt(String activity, String orderId) {
    int attempt = Activity.getExecutionContext().getInfo().getAttempt();
    if (attempt > 1) {
      log.warn("{} for order {} — attempt {}", activity, orderId, attempt);
    } else {
      log.info("{} for order {}", activity, orderId);
    }
  }
}
