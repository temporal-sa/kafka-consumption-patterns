package io.temporal.samples.kafka.common.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker-local store of live {@code KafkaConsumer} handles, keyed by instance ID.
 *
 * <p>A {@code KafkaConsumer} is not serializable, so it cannot be workflow state and cannot be
 * passed between activities. It lives here, in the worker JVM, and activities look it up by the
 * instance ID the workflow carries.
 *
 * <p>This is the single sharpest constraint in Pattern 2, and the reason that pattern pins its
 * workflow <em>and</em> its activities to one instance-scoped task queue served by exactly one
 * worker: every activity that touches the consumer must run in the JVM that holds it. Adding a
 * second worker to that queue would let activities land on a JVM with no consumer.
 *
 * <p>A miss is still handled safely rather than fatally — after a worker restart the consumer is
 * rebuilt and resumes from Kafka's committed offsets. Because commits happen only after the target
 * workflows have started, the worst case is redelivery, which deterministic workflow IDs absorb.
 */
public class ConsumerRegistry implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ConsumerRegistry.class);

  private final Map<String, KafkaConsumer<String, byte[]>> consumers = new ConcurrentHashMap<>();

  /** Returns the consumer for this instance, creating and subscribing it if absent. */
  public KafkaConsumer<String, byte[]> get(KafkaConsumerSettings settings) {
    return consumers.computeIfAbsent(
        settings.instanceId(),
        id -> {
          log.info(
              "Creating Kafka consumer for instance {} (group={}, topic={})",
              id,
              settings.groupId(),
              settings.topic());
          KafkaConsumer<String, byte[]> consumer = KafkaConsumers.create(settings);
          consumer.subscribe(java.util.List.of(settings.topic()));
          return consumer;
        });
  }

  public boolean has(String instanceId) {
    return consumers.containsKey(instanceId);
  }

  /** Closes and forgets one instance's consumer, leaving the group cleanly. */
  public void close(String instanceId) {
    KafkaConsumer<String, byte[]> consumer = consumers.remove(instanceId);
    if (consumer != null) {
      log.info("Closing Kafka consumer for instance {}", instanceId);
      consumer.close();
    }
  }

  @Override
  public void close() {
    consumers.keySet().forEach(this::close);
  }
}
