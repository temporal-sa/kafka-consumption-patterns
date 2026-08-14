package io.temporal.samples.kafka.worker.demo;

import io.temporal.samples.kafka.common.model.Address;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.model.OrderLine;
import io.temporal.samples.kafka.common.temporal.OrderEmailStarter;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Starts an {@code OrderEmailWorkflow} by hand, with no Kafka involved.
 *
 * <p>Exists so the target workflow and the chaos demo can be exercised before any consumer is built
 * (milestone M1). It remains useful afterwards for isolating whether a problem lies in consumption or
 * in the workflow itself — which only works because it goes through the same {@link
 * OrderEmailStarter} the consumers use, with the same idempotency semantics.
 */
@RestController
@RequestMapping("/demo")
public class DemoController {

  private final OrderEmailStarter starter;

  public DemoController(OrderEmailStarter starter) {
    this.starter = starter;
  }

  @PostMapping("/order-email")
  public Map<String, Object> startOrderEmail(@RequestBody(required = false) OrderCompleted body) {
    OrderCompleted event = body != null ? body : sampleOrder();
    OrderEmailStarter.StartOutcome outcome = starter.start(event);

    // LinkedHashMap rather than Map.of: runId is null when the event was deduplicated, and
    // Map.of rejects null values.
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("orderId", event.orderId());
    response.put("workflowId", outcome.workflowId());
    response.put("runId", outcome.runId());
    response.put("duplicate", outcome.duplicate());
    return response;
  }

  private OrderCompleted sampleOrder() {
    String orderId = "DEMO-" + System.currentTimeMillis();
    return new OrderCompleted(
        orderId,
        "CUST-1001",
        "Dana Rivera",
        "dana.rivera@example.com",
        List.of(
            new OrderLine("SKU-100", "Cast iron skillet", 1, new BigDecimal("48.00")),
            new OrderLine("SKU-220", "Chef's knife", 2, new BigDecimal("62.50"))),
        new BigDecimal("173.00"),
        new Address("144 Mercer St", null, "Seattle", "WA", "98101", "US"),
        Instant.now());
  }
}
