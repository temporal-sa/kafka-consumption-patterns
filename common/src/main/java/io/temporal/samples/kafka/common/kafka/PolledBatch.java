package io.temporal.samples.kafka.common.kafka;

import java.util.List;
import java.util.Map;

/**
 * The result of one poll: decoded orders, poison records, and the offsets to commit once both have
 * been dealt with.
 *
 * <p>Offsets are keyed {@code "topic-partition"} and hold the offset to commit (last read + 1).
 * Returning them rather than committing inside the poll is what lets Pattern 2's workflow decide
 * when the commit happens — after the workflow starts succeed, never before.
 */
public record PolledBatch(
    List<ConsumedOrder> orders, List<PoisonRecord> poison, Map<String, Long> offsets) {

  public boolean isEmpty() {
    return orders.isEmpty() && poison.isEmpty();
  }

  public int size() {
    return orders.size() + poison.size();
  }
}
