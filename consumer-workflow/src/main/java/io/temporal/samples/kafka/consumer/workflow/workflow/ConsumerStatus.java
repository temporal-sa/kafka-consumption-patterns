package io.temporal.samples.kafka.consumer.workflow.workflow;

/**
 * Snapshot of a consumer workflow's progress.
 *
 * @param historyLength events in the current run — watch this climb toward the continue-as-new
 *     threshold, which is the cost of this pattern's per-message visibility made visible
 */
public record ConsumerStatus(
    String instanceId,
    long messagesProcessed,
    long poisonRecords,
    long iterations,
    int continuations,
    long historyLength,
    boolean stopping) {}
