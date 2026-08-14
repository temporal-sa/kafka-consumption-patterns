package io.temporal.samples.kafka.common.kafka;

/**
 * Everything needed to build a Kafka consumer, in a form that can travel as a workflow or activity
 * argument.
 *
 * <p>Deliberately plain data. The {@code KafkaConsumer} itself is not serializable and cannot be
 * workflow state — it lives in a worker-local registry keyed by {@link #instanceId()}, and this is
 * what the workflow passes around instead.
 */
public record KafkaConsumerSettings(
    String instanceId,
    String bootstrapServers,
    String topic,
    String dltTopic,
    String groupId,
    long pollTimeoutMs,
    int maxRecordsPerPoll) {}
