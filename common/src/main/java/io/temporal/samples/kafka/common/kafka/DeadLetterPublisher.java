package io.temporal.samples.kafka.common.kafka;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes undecodable records to the dead-letter topic for patterns 2 and 3.
 *
 * <p>Pattern 1 gets this from spring-kafka's {@code DeadLetterPublishingRecoverer}; this is the
 * equivalent for the two patterns that drive Kafka directly, so all three behave the same way
 * (FR-X4). The payload is written back unchanged, on the same partition, so ordering within a key is
 * preserved on the DLT too.
 *
 * <p>Note the DLT is at-least-once like everything else here: if the process dies after publishing
 * but before the source offset is committed, the record is redelivered and dead-lettered again.
 * Whatever reads the DLT must tolerate duplicates.
 */
public class DeadLetterPublisher implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

  private final Producer<String, byte[]> producer;

  public DeadLetterPublisher(String bootstrapServers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    this.producer = new KafkaProducer<>(props);
  }

  public void publish(String dltTopic, List<PoisonRecord> poison) {
    for (PoisonRecord record : poison) {
      log.error(
          "Routing record to {} — topic={} partition={} offset={} reason={}",
          dltTopic,
          record.topic(),
          record.partition(),
          record.offset(),
          record.failureReason());
      producer.send(
          new ProducerRecord<>(
              dltTopic,
              record.partition(),
              record.key(),
              record.rawValue() == null
                  ? null
                  : record.rawValue().getBytes(StandardCharsets.UTF_8)));
    }
    // Flush before returning: the caller commits the source offset next, and must not move past a
    // record that has not actually reached the DLT.
    producer.flush();
  }

  @Override
  public void close() {
    producer.close();
  }
}
