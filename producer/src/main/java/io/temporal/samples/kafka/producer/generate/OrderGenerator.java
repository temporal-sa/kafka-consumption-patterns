package io.temporal.samples.kafka.producer.generate;

import io.temporal.samples.kafka.common.model.Address;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import io.temporal.samples.kafka.common.model.OrderLine;
import io.temporal.samples.kafka.producer.config.ProducerProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Synthetic order generator, seeded so a run can be reproduced exactly (PRD FR-P4).
 *
 * <p>Order IDs are sequential rather than random, which makes it easy to spot gaps or duplicates by
 * eye when comparing what was produced against the workflows that ran.
 *
 * <h2>Why order IDs are namespaced per run</h2>
 *
 * The order ID determines the workflow ID, and every consumer deduplicates on it. A plain counter
 * would therefore restart at {@code ORD-000001} on every producer restart, and each of those events
 * would be correctly rejected as a duplicate of the previous run's work — producing a run in which
 * messages are consumed, no workflows start, and everything looks broken while behaving exactly as
 * designed. It also quietly ruins load tests, which would measure deduplication rather than
 * throughput.
 *
 * <p>So IDs carry a run token by default: {@code ORD-{prefix}-{sequence}}. Leave {@code
 * app.producer.order-id-prefix} unset and each run gets a fresh token from its start time. Set it
 * alongside a fixed {@code seed} to replay a run deliberately — which is also how you generate
 * genuine cross-run duplicates on purpose.
 */
@Component
public class OrderGenerator {

  private static final Logger log = LoggerFactory.getLogger(OrderGenerator.class);

  private static final String[] FIRST_NAMES = {
    "Dana", "Priya", "Marcus", "Ines", "Tomas", "Aiko", "Ruth", "Omar", "Lena", "Kwame"
  };
  private static final String[] LAST_NAMES = {
    "Rivera", "Okafor", "Lindqvist", "Haddad", "Moreau", "Tanaka", "Bell", "Novak", "Ferreira", "Cole"
  };
  private static final String[][] CATALOG = {
    {"SKU-100", "Cast iron skillet", "48.00"},
    {"SKU-220", "Chef's knife", "62.50"},
    {"SKU-310", "Espresso grinder", "129.00"},
    {"SKU-415", "Stockpot, 8qt", "74.25"},
    {"SKU-502", "Cutting board", "31.75"},
    {"SKU-618", "Kitchen scale", "27.00"}
  };
  private static final String[][] CITIES = {
    {"Seattle", "WA", "98101"},
    {"Austin", "TX", "78701"},
    {"Boston", "MA", "02108"},
    {"Denver", "CO", "80202"},
    {"Chicago", "IL", "60601"}
  };

  private final Random random;
  private final AtomicLong sequence = new AtomicLong(1);
  private final List<String> issuedOrderIds = new ArrayList<>();
  private final String orderIdPrefix;

  public OrderGenerator(ProducerProperties properties) {
    this.random = new Random(properties.getSeed());
    this.orderIdPrefix =
        StringUtils.hasText(properties.getOrderIdPrefix())
            ? properties.getOrderIdPrefix()
            // Base-36 epoch seconds: short enough to stay readable in a workflow ID, and
            // monotonic, so runs sort in the order they happened.
            : Long.toString(Instant.now().getEpochSecond(), 36);
    log.info(
        "Generating order IDs as ORD-{}-nnnnnn (set app.producer.order-id-prefix to replay a run)",
        orderIdPrefix);
  }

  public synchronized OrderCompleted next() {
    String orderId = String.format("ORD-%s-%06d", orderIdPrefix, sequence.getAndIncrement());
    issuedOrderIds.add(orderId);
    return build(orderId);
  }

  /**
   * Rebuilds an event for an order ID already published, to exercise the idempotent-start path
   * (PRD FR-P5). Returns null until at least one order exists.
   */
  public synchronized OrderCompleted replayRandomPrevious() {
    if (issuedOrderIds.isEmpty()) {
      return null;
    }
    return build(issuedOrderIds.get(random.nextInt(issuedOrderIds.size())));
  }

  private OrderCompleted build(String orderId) {
    String first = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
    String last = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
    String customerId = String.format("CUST-%04d", 1000 + random.nextInt(9000));

    int lineCount = 1 + random.nextInt(3);
    List<OrderLine> lines = new ArrayList<>(lineCount);
    BigDecimal total = BigDecimal.ZERO;
    for (int i = 0; i < lineCount; i++) {
      String[] item = CATALOG[random.nextInt(CATALOG.length)];
      int quantity = 1 + random.nextInt(3);
      BigDecimal unitPrice = new BigDecimal(item[2]);
      lines.add(new OrderLine(item[0], item[1], quantity, unitPrice));
      total = total.add(unitPrice.multiply(BigDecimal.valueOf(quantity)));
    }

    String[] city = CITIES[random.nextInt(CITIES.length)];
    Address address =
        new Address(
            (100 + random.nextInt(900)) + " Mercer St", null, city[0], city[1], city[2], "US");

    return new OrderCompleted(
        orderId,
        customerId,
        first + " " + last,
        (first + "." + last).toLowerCase() + "@example.com",
        lines,
        total.setScale(2, RoundingMode.HALF_UP),
        address,
        Instant.now());
  }
}
