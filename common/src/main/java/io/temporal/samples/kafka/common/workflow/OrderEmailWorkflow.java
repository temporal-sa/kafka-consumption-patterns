package io.temporal.samples.kafka.common.workflow;

import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * The shared target workflow — Customer ABC's scenario from the reference architecture.
 *
 * <p>Looks up the order and shipping details, generates an invoice, and sends the order email. Every
 * downstream call is an activity with retries, so an email provider outage or a slow customer
 * database parks the workflow rather than losing the event. That durability is the point of the
 * whole integration.
 *
 * <p>All three consumption patterns start <em>this</em> workflow on <em>one</em> task queue served
 * by one worker (PRD FR-X1). Do not fork it per pattern — the comparison depends on it being
 * identical.
 */
@WorkflowInterface
public interface OrderEmailWorkflow {

  @WorkflowMethod
  OrderEmailResult sendOrderEmail(OrderCompleted event);
}
