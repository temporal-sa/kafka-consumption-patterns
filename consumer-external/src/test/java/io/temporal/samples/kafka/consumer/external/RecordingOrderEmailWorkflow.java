package io.temporal.samples.kafka.consumer.external;

import io.temporal.samples.kafka.common.config.TaskQueues;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.workflow.OrderEmailResult;
import io.temporal.samples.kafka.common.workflow.OrderEmailWorkflow;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test stand-in for the real workflow, recording how many executions each order actually got.
 *
 * <p>Registered on the same task queue as the production implementation, so the consumer under test
 * is exercised exactly as it would be in the real system — the substitution happens on the worker
 * side, not in the consumer.
 */
@WorkflowImpl(taskQueues = TaskQueues.ORDER_EMAIL)
public class RecordingOrderEmailWorkflow implements OrderEmailWorkflow {

  /** orderId -> number of workflow executions that actually ran for it. */
  static final ConcurrentHashMap<String, Integer> EXECUTIONS = new ConcurrentHashMap<>();

  @Override
  public OrderEmailResult sendOrderEmail(OrderCompleted event) {
    EXECUTIONS.merge(event.orderId(), 1, Integer::sum);
    return new OrderEmailResult(
        event.orderId(),
        "msg-test",
        "invoice://test",
        Instant.ofEpochMilli(Workflow.currentTimeMillis()));
  }
}
