# consumer-workflow — Pattern 2: Workflow

The consumer loop *is* a Temporal Workflow. It polls Kafka, starts a target workflow per message, and
commits offsets — all through activities, so **every message consumed appears in workflow history**.

```bash
mvn -f consumer-workflow/pom.xml spring-boot:run    # http://localhost:8083
```

Requires Kafka, a Temporal dev server, and `order-email-worker` running. Unlike Pattern 1, this
application **is** a worker: it hosts the consumer workflow and its activities, and starts the
workflow on boot.

## The loop

```java
activities.subscribe(settings);

while (!stopRequested) {
  PolledBatch batch = activities.poll(settings);
  if (!batch.isEmpty()) {
    activities.startTargetWorkflows(batch.orders());
    activities.deadLetter(settings, batch.poison());
    activities.commitOffsets(settings, batch.offsets());   // only after the hand-off succeeded
  }
  if (shouldContinueAsNew()) { /* carry counters forward */ }
}

activities.close(settings);
```

Note what is *absent*: no try/catch around the loop, no reconnection logic, no retry bookkeeping.
Activity retry policies cover all of it and the loop's position is durable. That is the argument for
putting a consumer loop in a workflow at all.

## The constraint that shapes everything: one instance, one queue, one worker

A `KafkaConsumer` is not serializable, so it cannot be workflow state. It lives in a worker-local
`ConsumerRegistry`, and every activity that touches it must run in that JVM.

This module solves that structurally rather than defensively:

```yaml
spring.temporal.workers:
  - task-queue: kafka-consumer-${app.consumer.instance-id}
    workflow-classes: [ KafkaConsumerWorkflowImpl ]     # workflow AND
    activity-beans:   [ kafkaConsumerActivities ]       # activities, same queue
```

One instance ⇒ one task queue ⇒ one worker ⇒ one JVM. There is no routing race and no cross-worker
cache miss, because there is nowhere else for an activity to go.

**Do not run two processes with the same `instance-id`.** Both would poll the same queue, and
activities could land on the JVM that does not hold the consumer. This inverts the usual Temporal
instinct that more workers on a queue is always safe — here a second worker is a split-brain.

Scale out by adding *instances*:

```bash
CONSUMER_INSTANCE_ID=2 SERVER_PORT=8093 mvn -f consumer-workflow/pom.xml spring-boot:run
```

Each gets its own queue, worker, and workflow execution. They share one consumer group, so Kafka
splits the partitions between them — and, as everywhere else in this repo, the partition count is the
ceiling.

Worker restart is the one remaining path that rebuilds a consumer: the cached handle dies with the
process, and the restarted worker recreates it on the next `poll`, resuming from Kafka's committed
offsets. Because commits happen only after the workflow starts succeed, the worst case is redelivery
— which deterministic workflow IDs absorb.

## Continue-as-new

Per-message visibility means history grows with every message, and history is finite.

The trigger is `Workflow.getInfo().isContinueAsNewSuggested()` — the service's own signal, rather
than a hardcoded threshold that would go stale as limits change. `MAX_ITERATIONS_PER_RUN` (2,000) is
a secondary backstop so a demo can force a continuation without generating thousands of events.

The consumer is deliberately **not** closed before continuing. The next run lands on the same queue
and worker, so keeping it open avoids a pointless consumer-group rebalance on every continuation.

Watch it happen:

```bash
curl -s localhost:8083/consumer | jq
# { "instanceId":"1", "messagesProcessed":412, "iterations":133,
#   "continuations":0, "historyLength":807, "stopping":false }
```

`historyLength` climbing is this pattern's cost made visible.

## Control

| Call | Effect |
| :- | :- |
| `GET /consumer` | Query the running workflow — no Kafka involved, no effect on the loop |
| `DELETE /consumer` | Signal the loop to finish its iteration, close the consumer, and complete |

Stopping is graceful: the consumer leaves its group cleanly rather than waiting to be timed out, so
remaining instances rebalance immediately.

## Cost

**3 Actions per message** — poll + start + commit — which is exactly what the reference architecture
publishes, because `batch-size` is pinned to 1.

Raising it amortises the loop across the batch: at 50 records per poll the loop costs roughly 0.06
Actions per message, which substantially changes that conclusion. It is a single constant,
`ConsumerWorkflowProperties.DEFAULT_BATCH_SIZE`. **If you change it, the 3-Actions-per-message row in
the reference architecture doc becomes wrong and must be updated to match.**

### ⚠️ Batch size is a throughput decision, not just a cost decision

This is the most consequential measured finding in the repo. Same rate, same everything else, only
`batch-size` changed (`scripts/load-test.sh`, 50 events/s, single instance):

| batch-size | throughput | kept up? | final lag | p50 latency |
| :- | :- | :- | :- | :- |
| **1** | 3.4 msg/s | **no** | 2,444 | **25.8 s** |
| **50** | 51.7 msg/s | yes | 35 | **0.57 s** |

**15× the throughput and 45× better latency.**

The cause is structural, not incidental. At one record per poll, every message costs three
*serialized* activity round-trips — poll, start, commit — each a full workflow-task/activity-task
cycle. That puts a hard floor of a few messages per second on a single instance, no matter how fast
Kafka or the target workflow is. Batching amortises those three round-trips over the whole batch.

The consequence is worth stating plainly: **at batch-size 1 this pattern cannot be scaled into
viability.** Instances are capped by the partition count, so on a 6-partition topic the ceiling is
roughly 6 × 3.4 ≈ 20 msg/s for the entire pattern. It cannot reach 50 events/s at all.

The reference architecture's cost table compares the patterns on Actions per message and says
nothing about throughput. On that measure alone, batch-size 1 looks merely expensive. It is also,
at one record per poll, unusable above trivial volumes — and that is the finding this repo exists to
surface.

So: **pin `batch-size` to 1 only to reproduce the published cost figure. For anything real, batch.**
Reproduce it yourself:

```bash
PATTERNS=wf RATES=50 BATCH_SIZE=1  scripts/load-test.sh
PATTERNS=wf RATES=50 BATCH_SIZE=50 scripts/load-test.sh
```

There is also an idle cost the other patterns do not have: an empty poll still spends one Action, so
a quiet topic costs about `60000 / poll-timeout-ms` Actions per minute doing nothing. Raise
`poll-timeout-ms` to make an idle consumer cheaper, at the cost of latency when traffic resumes.

## Configuration

| Property | Default | Meaning |
| :- | :- | :- |
| `app.consumer.instance-id` | `1` | Drives task queue, workflow ID, and consumer handle key |
| `app.consumer.topic` | `orders.completed` | Source topic |
| `app.consumer.dlt-topic` | `orders.completed.DLT` | Dead-letter destination |
| `app.consumer.group-id` | `temporal-workflow-consumer` | Own group, so all three patterns run at once |
| `app.consumer.poll-timeout-ms` | `5000` | Poll block time; also the idle Action cost |
| `app.consumer.batch-size` | `1` | Pinned — see Cost above |

## When to choose this

When per-message visibility in workflow history is a genuine requirement: audit, compliance, or
debugging an unreliable upstream. You are paying 3× the Actions and a one-to-one binding of instance
to queue to worker to workflow execution, and getting a complete, queryable record of every message
the consumer touched.

If you do not need that record, Pattern 1 or 3 will cost you less and operate more simply.
