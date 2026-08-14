package io.temporal.samples.kafka.worker.workflow;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.samples.kafka.common.config.TaskQueues;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.model.OrderLine;
import io.temporal.samples.kafka.common.workflow.EmailRequest;
import io.temporal.samples.kafka.common.workflow.OrderDetails;
import io.temporal.samples.kafka.common.workflow.OrderEmailActivities;
import io.temporal.samples.kafka.common.workflow.OrderEmailResult;
import io.temporal.samples.kafka.common.workflow.OrderEmailWorkflow;
import io.temporal.samples.kafka.common.workflow.ShippingDetails;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;

@WorkflowImpl(taskQueues = TaskQueues.ORDER_EMAIL)
public class OrderEmailWorkflowImpl implements OrderEmailWorkflow {

  private static final Logger log = Workflow.getLogger(OrderEmailWorkflowImpl.class);

  /**
   * Defaults for the two database lookups: retry indefinitely with capped backoff.
   *
   * <p>Unlimited attempts is the deliberate choice for this sample. Customer ABC's problem was that
   * a slow or unavailable customer database broke order emails; here it merely delays them. Toggle
   * the chaos knobs (see {@code /chaos}) and watch the workflow park in retry rather than fail.
   */
  private static final ActivityOptions DEFAULT_OPTIONS =
      ActivityOptions.newBuilder()
          .setStartToCloseTimeout(Duration.ofSeconds(10))
          .setRetryOptions(
              RetryOptions.newBuilder()
                  .setInitialInterval(Duration.ofSeconds(1))
                  .setBackoffCoefficient(2.0)
                  .setMaximumInterval(Duration.ofSeconds(10))
                  .setMaximumAttempts(0) // unlimited
                  .build())
          .build();

  /**
   * Per-activity overrides, keyed by activity type name (method name, first letter capitalized).
   *
   * <p>One stub with per-activity options beats several stubs — the differences between these
   * activities stay visible in one place.
   */
  private static final Map<String, ActivityOptions> PER_ACTIVITY_OPTIONS =
      Map.of(
          "GenerateInvoice",
          ActivityOptions.newBuilder()
              // Invoice rendering is slow by design; give it room before declaring a timeout.
              .setStartToCloseTimeout(Duration.ofMinutes(2))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(5).build())
              .build(),
          "SendEmail",
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setBackoffCoefficient(2.0)
                      .setMaximumInterval(Duration.ofSeconds(30))
                      // The third-party email provider is the thing most likely to be down.
                      // Never give up: the email is delivered late rather than lost, which is
                      // exactly the guarantee the reference architecture is arguing for.
                      .setMaximumAttempts(0)
                      .build())
              .build());

  private final OrderEmailActivities activities =
      Workflow.newActivityStub(OrderEmailActivities.class, DEFAULT_OPTIONS, PER_ACTIVITY_OPTIONS);

  @Override
  public OrderEmailResult sendOrderEmail(OrderCompleted event) {
    log.info("Starting order email workflow for order {}", event.orderId());

    OrderDetails order = activities.lookupOrder(event.orderId());
    ShippingDetails shipping = activities.lookupShippingDetails(event.orderId());
    String invoiceRef = activities.generateInvoice(event, order);

    String messageId =
        activities.sendEmail(
            new EmailRequest(
                event.orderId(),
                order.customerEmail(),
                order.customerName(),
                "Your order " + event.orderId() + " is complete",
                renderBody(event, order, shipping),
                invoiceRef));

    log.info("Order email sent for order {} (messageId={})", event.orderId(), messageId);
    // Workflow.currentTimeMillis(), never Instant.now() — workflow code must be deterministic
    // so that replay produces the same result.
    return new OrderEmailResult(
        event.orderId(), messageId, invoiceRef, Instant.ofEpochMilli(Workflow.currentTimeMillis()));
  }

  private String renderBody(OrderCompleted event, OrderDetails order, ShippingDetails shipping) {
    StringBuilder body = new StringBuilder();
    body.append("Hi ").append(order.customerName()).append(",\n\n");
    body.append("Thanks for your order ").append(event.orderId()).append(".\n\n");
    for (OrderLine line : event.lines()) {
      body.append("  ")
          .append(line.quantity())
          .append(" x ")
          .append(line.description())
          .append(" @ ")
          .append(line.unitPrice())
          .append('\n');
    }
    body.append("\nTotal: ").append(order.total()).append('\n');
    body.append("Shipping via ")
        .append(shipping.carrier())
        .append(" (tracking ")
        .append(shipping.trackingNumber())
        .append("), estimated delivery ")
        .append(shipping.estimatedDelivery())
        .append(".\n");
    return body.toString();
  }
}
