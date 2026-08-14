package io.temporal.samples.kafka.common.kafka;

import io.temporal.samples.kafka.common.json.Json;
import io.temporal.samples.kafka.common.model.OrderCompleted;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Raw {@code KafkaConsumer} construction and record decoding, shared by patterns 2 and 3.
 *
 * <p>Both of those patterns drive poll and commit explicitly from inside activities, so neither can
 * use spring-kafka's listener container the way Pattern 1 does. Sharing this code means the only
 * thing that differs between them is <em>where the loop lives</em> — which is the whole point of the
 * comparison.
 *
 * <p>Values are read as raw bytes and decoded here rather than by a deserializer, so a malformed
 * payload becomes a {@link PoisonRecord} the caller can route, instead of an exception thrown inside
 * the poll loop that would retry the same record forever.
 */
public final class KafkaConsumers {

  public static KafkaConsumer<String, byte[]> create(KafkaConsumerSettings settings) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Offsets are committed explicitly, only after the target workflows have been started.
    // Auto-commit would advance them on a timer regardless, which is how events get dropped.
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.maxRecordsPerPoll());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new KafkaConsumer<>(props);
  }

  /** Polls once and splits the result into decodable orders and poison, plus offsets to commit. */
  public static PolledBatch poll(KafkaConsumer<String, byte[]> consumer, Duration timeout) {
    ConsumerRecords<String, byte[]> records = consumer.poll(timeout);

    List<ConsumedOrder> orders = new ArrayList<>();
    List<PoisonRecord> poison = new ArrayList<>();
    Map<String, Long> offsets = new LinkedHashMap<>();

    for (ConsumerRecord<String, byte[]> record : records) {
      // Commit position is "next offset to read", hence +1.
      offsets.put(record.topic() + "-" + record.partition(), record.offset() + 1);
      try {
        OrderCompleted event =
            Json.newObjectMapper().readValue(record.value(), OrderCompleted.class);
        orders.add(
            new ConsumedOrder(record.topic(), record.partition(), record.offset(), event));
      } catch (Exception e) {
        poison.add(
            new PoisonRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8),
                e.getClass().getSimpleName() + ": " + e.getMessage()));
      }
    }

    return new PolledBatch(orders, poison, offsets);
  }

  /** Commits the offsets from a {@link PolledBatch}. */
  public static void commit(KafkaConsumer<String, byte[]> consumer, Map<String, Long> offsets) {
    if (offsets.isEmpty()) {
      return;
    }
    Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> toCommit =
        new HashMap<>();
    offsets.forEach(
        (key, offset) -> {
          int split = key.lastIndexOf('-');
          String topic = key.substring(0, split);
          int partition = Integer.parseInt(key.substring(split + 1));
          toCommit.put(
              new TopicPartition(topic, partition),
              new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset));
        });
    consumer.commitSync(toCommit);
  }

  private KafkaConsumers() {}
}
