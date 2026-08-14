package io.temporal.samples.kafka.common.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.samples.kafka.common.model.OrderCompleted;

/** Downstream calls made by {@link OrderEmailWorkflow}. Each is independently retryable. */
@ActivityInterface
public interface OrderEmailActivities {

  /** Reads the order from the (flaky, sometimes slow) order database. */
  OrderDetails lookupOrder(String orderId);

  /** Reads shipping/tracking details for the order. */
  ShippingDetails lookupShippingDetails(String orderId);

  /** Renders a customer invoice and returns a reference to it. Deliberately slow. */
  String generateInvoice(OrderCompleted event, OrderDetails order);

  /**
   * Sends the order email via the third-party provider and returns its message ID.
   *
   * <p>The provider is the component most likely to be down in the demo; the workflow retries this
   * indefinitely so that the email is eventually delivered rather than dropped.
   */
  String sendEmail(EmailRequest request);
}
