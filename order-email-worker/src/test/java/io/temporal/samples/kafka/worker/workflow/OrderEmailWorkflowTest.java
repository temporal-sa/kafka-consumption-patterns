package io.temporal.samples.kafka.worker.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import io.temporal.samples.kafka.common.model.Address;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.model.OrderLine;
import io.temporal.samples.kafka.common.workflow.EmailRequest;
import io.temporal.samples.kafka.common.workflow.OrderDetails;
import io.temporal.samples.kafka.common.workflow.OrderEmailActivities;
import io.temporal.samples.kafka.common.workflow.OrderEmailResult;
import io.temporal.samples.kafka.common.workflow.OrderEmailWorkflow;
import io.temporal.samples.kafka.common.workflow.ShippingDetails;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.TestWorkflowExtension;
import io.temporal.worker.Worker;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class OrderEmailWorkflowTest {

  @RegisterExtension
  static final TestWorkflowExtension extension =
      TestWorkflowExtension.newBuilder()
          .setWorkflowTypes(OrderEmailWorkflowImpl.class)
          .setDoNotStart(true)
          .build();

  @Test
  void sendsOrderEmail(
      TestWorkflowEnvironment env, Worker worker, OrderEmailWorkflow workflow) {
    worker.registerActivitiesImplementations(new StubActivities(0));
    env.start();

    OrderEmailResult result = workflow.sendOrderEmail(sampleOrder());

    assertThat(result.orderId()).isEqualTo("ORD-000001");
    assertThat(result.emailMessageId()).isEqualTo("msg-1");
    assertThat(result.invoiceRef()).startsWith("invoice://");
  }

  /**
   * The durability claim, asserted rather than described: the email provider fails the first three
   * attempts and the workflow still completes, with no intervention and nothing lost.
   *
   * <p>Retry backoff is skipped by the test environment's virtual clock, so this runs instantly.
   */
  @Test
  void retriesUntilTheEmailProviderRecovers(
      TestWorkflowEnvironment env, Worker worker, OrderEmailWorkflow workflow) {
    StubActivities activities = new StubActivities(3);
    worker.registerActivitiesImplementations(activities);
    env.start();

    OrderEmailResult result = workflow.sendOrderEmail(sampleOrder());

    assertThat(result.emailMessageId()).isEqualTo("msg-4");
    assertThat(activities.emailAttempts.get()).isEqualTo(4);
  }

  private OrderCompleted sampleOrder() {
    return new OrderCompleted(
        "ORD-000001",
        "CUST-1001",
        "Dana Rivera",
        "dana.rivera@example.com",
        List.of(new OrderLine("SKU-100", "Cast iron skillet", 1, new BigDecimal("48.00"))),
        new BigDecimal("48.00"),
        new Address("144 Mercer St", null, "Seattle", "WA", "98101", "US"),
        Instant.parse("2026-08-12T15:00:00Z"));
  }

  /** Fails {@code emailFailures} times before succeeding, so retry behaviour is observable. */
  private static final class StubActivities implements OrderEmailActivities {

    private final int emailFailures;
    private final AtomicInteger emailAttempts = new AtomicInteger();

    StubActivities(int emailFailures) {
      this.emailFailures = emailFailures;
    }

    @Override
    public OrderDetails lookupOrder(String orderId) {
      return new OrderDetails(
          orderId, "Dana Rivera", "dana.rivera@example.com", new BigDecimal("48.00"), 1);
    }

    @Override
    public ShippingDetails lookupShippingDetails(String orderId) {
      return new ShippingDetails(
          orderId, "ACME Freight", "TRK-" + orderId, LocalDate.parse("2026-08-16"));
    }

    @Override
    public String generateInvoice(OrderCompleted event, OrderDetails order) {
      return "invoice://" + event.orderId() + "/test";
    }

    @Override
    public String sendEmail(EmailRequest request) {
      int attempt = emailAttempts.incrementAndGet();
      if (attempt <= emailFailures) {
        throw new IllegalStateException("email provider unavailable (attempt " + attempt + ")");
      }
      return "msg-" + attempt;
    }
  }
}
