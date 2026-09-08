# consumer-activity — Pattern 3: Long-Running Activity

A Kafka consumer that lives inside a Temporal activity which never returns. Temporal owns its
lifecycle — visible, supervised, restartable — without a separate deployment to operate.

```bash
mvn -f consumer-activity/pom.xml spring-boot:run    # http://localhost:8084
```

Requires Kafka, a Temporal dev server, and `order-email-worker` running.

This is the pattern Customer ABC chose in the reference architecture, and it is usually the right one
when the goal is *one less thing to operate*.

## The shape

The workflow is deliberately trivial — it exists only to own the activities' lifecycle:

```java
for (int i = 0; i < parallelConsumers; i++) {
  consumers.add(Async.procedure(consume::consume, perConsumerSettings(i)));
}
Promise.allOf(consumers).get();   // completes only on cancellation
```

All the work is in the activity, which loops forever:

```java
try (KafkaConsumer<String, byte[]> consumer = KafkaConsumers.create(settings)) {
  consumer.subscribe(List.of(settings.topic()));
  heartbeat(...);                       // before the first poll, so a quiet topic still looks alive
  while (true) {
    PolledBatch batch = KafkaConsumers.poll(consumer, pollTimeout);
    startTargetWorkflows(batch.orders());
    deadLetter(batch.poison());
    KafkaConsumers.commit(consumer, batch.offsets());   // only after the hand-off
    heartbeat(...);
  }
}
```

The consumer is a **local variable**, not a registry entry. It is created, used, and closed inside one
invocation, so nothing else ever needs to reach it. That is why this pattern needs neither Pattern 2's
instance-scoped task queue nor its one-worker-per-queue rule — a simpler deployment story, bought
with less visibility.

## Activity options are the whole design

```java
.setStartToCloseTimeout(Duration.ofDays(365))   // NOT a deadline for "how long the work takes"
.setHeartbeatTimeout(Duration.ofSeconds(30))    // the real liveness check
.setRetryOptions(... setMaximumAttempts(0))     // a broker outage pauses consumption, never ends it
```

These are the settings people most often get wrong.

`startToCloseTimeout` is an upper bound on the *life of one consumer*, not on a unit of work — it has
to be long. Liveness is enforced by `heartbeatTimeout`: stop heartbeating for 30 seconds and Temporal
treats the activity as dead and retries it, which is how a wedged or killed consumer is replaced
automatically with nobody paged.

Heartbeats fire **every iteration, including idle ones**. The SDK throttles them to a fraction of the
heartbeat timeout, so this costs far fewer Actions than it looks like — do not hand-roll throttling
on top of it.

## ⚠️ The trap: worker execution slots

**A long-running activity never finishes, so it never releases its execution slot.** With SDK
defaults, a handful of consumers permanently occupy the worker's activity slots and every other
activity queues forever. The worker looks healthy and does nothing.

Handled declaratively, where a reader looking for it would expect it:

```yaml
spring.temporal.workers:
  - task-queue: kafka-consumer-activity
    capacity:
      max-concurrent-activity-executors: 20      # >> parallel-consumers
      max-concurrent-activity-task-pollers: 10   # or nobody is left to pick up new tasks
```

Keep `max-concurrent-activity-executors` comfortably above `app.consumer.parallel-consumers`. The
headroom is what any *other* activity on this queue runs in. Better still, give consumer activities
their own worker and task queue, as this module does.

## Shutdown and recovery

Cancellation propagates into the next heartbeat, which throws `ActivityCompletionException`. The
try-with-resources closes the consumer, so it leaves its group cleanly instead of waiting out the
session timeout before the group rebalances.

Kill the process outright and nothing needs restarting by hand: the activity stops heartbeating,
Temporal retries it after `heartbeatTimeout`, and the consumer resumes from Kafka's committed
offsets. Offsets commit only after target workflows are started, so the worst case is redelivery,
which deterministic workflow IDs absorb.

### ⚠️ Killing the process does not stop the consumer

That auto-recovery is the pattern's best feature and its sharpest edge, because it does not switch
off when you want it to.

**The consumer's lifecycle belongs to Temporal, not to your JVM.** Kill the process and the workflow
survives; the activity merely stops heartbeating, and the next worker to poll
`kafka-consumer-activity` picks it up and resumes consuming — under its *original* settings, topic
and consumer group included.

This bites hardest during development and demos, where you restart the app with changed settings:

```
run 1: parallel-consumers=1   → kill process → workflow still running
run 2: parallel-consumers=3   → new worker resurrects run 1's consumer too  (4 consumers live)
run 3: parallel-consumers=6   → ...now 10
run 4: parallel-consumers=9   → 19 consumers, against 20 activity slots. Nothing starts.
```

Building `scripts/demo-partition-ceiling.sh` walked straight into exactly this, and the symptom was
not "too many consumers" — it was a consumer that silently never started, plus throughput figures
polluted by ghosts of earlier runs.

To actually stop a consumer, terminate its workflow:

```bash
temporal workflow terminate --workflow-id kafka-consumer-activity-1 --reason "done"
```

Find orphans:

```bash
temporal workflow list \
  --query "WorkflowType = 'KafkaConsumerActivityWorkflow' AND ExecutionStatus = 'Running'"
```

The demo script does both automatically — it terminates the workflow before killing the process, and
sweeps orphans from interrupted runs on startup.

## Visibility

"Some" — which is the honest answer.

Nothing per-message reaches workflow history, because a message is consumed and its workflow started
entirely inside one activity invocation. What you *do* get is each consumer visible as a pending
activity with live heartbeat details:

```
Pending Activities: 2
  Type               Consume
  State              Started
  Attempt            1
  MaximumAttempts    0
  LastHeartbeatTime  22 seconds ago
```

The heartbeat details carry the instance ID, messages processed, and committed offsets. Be clear
about what they are: **observability, not correctness**. Recovery comes from Kafka's committed
offsets, not from these details.

## Scaling

`app.consumer.parallel-consumers` launches N consume activities from the one workflow, each a Kafka
consumer in the group. This is the "run multiple activities in parallel" answer from the trade-off
table.

It hits the same ceiling as everything else here: past the topic's partition count the extra
activities sit idle, holding worker slots and doing nothing.

## Cost

**1 Action per message**, plus one per throttled heartbeat — the same per-message cost as Pattern 1,
with the heartbeats as the premium for Temporal-managed lifecycle.

## Configuration

| Property | Default | Meaning |
| :- | :- | :- |
| `app.consumer.instance-id` | `1` | Suffixed per parallel consumer |
| `app.consumer.topic` | `orders.completed` | Source topic |
| `app.consumer.dlt-topic` | `orders.completed.DLT` | Dead-letter destination |
| `app.consumer.group-id` | `temporal-activity-consumer` | Own group, so all three patterns run at once |
| `app.consumer.poll-timeout-ms` | `5000` | Poll block time |
| `app.consumer.max-records-per-poll` | `50` | Records per poll — no Action cost implication here, unlike Pattern 2 |
| `app.consumer.parallel-consumers` | `2` | Consumers in the group; must stay well below the executor capacity above |

## When to choose this

When you want the consumer's lifecycle managed and supervised by Temporal — restarted automatically,
visible in the UI, with no separate consumer deployment to build and operate — and you do not need a
per-message record.

Accept heartbeat configuration and worker slot headroom as the price. If you need per-message
history, that is Pattern 2. If you would rather operate consumers with the deployment tooling you
already have, that is Pattern 1.

## Pattern 3b — the same activity, without the workflow

Not implemented in this repo. Read this section, then decide whether to reach for it.

Notice what `KafkaConsumerActivityWorkflowImpl` actually does: a `for` loop and
`Promise.allOf(...).get()`. It orchestrates nothing. It exists because, until recently, an activity
had to hang off a workflow. [Standalone
Activities](https://docs.temporal.io/standalone-activity) remove that requirement — an activity
becomes a top-level execution started straight from a client, with the same durability, retries,
heartbeats, and cancellation.

The activity body needs **zero changes**. `KafkaConsumeActivityImpl` touches Temporal in exactly two
places — `Activity.getExecutionContext().getInfo().getAttempt()` and `.heartbeat(...)` — and both are
valid standalone. Only the launch path differs, so `ConsumerBootstrap` absorbs the whole change:

```java
ActivityClient activities =
    ActivityClient.newInstance(
        service, ActivityClientOptions.newBuilder().setNamespace(namespace).build());

for (int i = 0; i < parallelConsumers; i++) {
  activities.start(
      KafkaConsumeActivity.class,
      KafkaConsumeActivity::consume,
      StartActivityOptions.newBuilder()
          .setId("kafka-consumer-" + settings.groupId() + "-" + i)   // one ID per consumer
          .setTaskQueue(TASK_QUEUE)
          .setStartToCloseTimeout(Duration.ofDays(365))
          .setHeartbeatTimeout(Duration.ofSeconds(30))
          .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(0).build())
          // Replaces catching WorkflowExecutionAlreadyStarted: restarting the app reattaches
          // to the running consumer instead of erroring.
          .setIdConflictPolicy(ActivityIdConflictPolicy.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING)
          .build(),
      perConsumer(i));
}
```

`KafkaConsumerActivityWorkflow` and its `Impl` delete entirely. The `@ActivityImpl(taskQueues = ...)`
registration and the worker capacity configuration stay exactly as they are.

### What you gain

**Each consumer becomes individually addressable.** This is the real prize, and it lands directly on
the footgun documented above. Today the fleet is one workflow: you cannot add a consumer, drop one,
or change one's settings without terminating all N. With an activity ID per consumer, startup becomes
a reconciliation — list what is running for this group, start what is missing, terminate the extras —
which fixes the run-1/run-2/run-3 ghost accumulation at the root instead of sweeping up after it.

**Idempotent start moves into the platform.** `USE_EXISTING` is the conflict policy version of the
`catch (WorkflowExecutionAlreadyStarted)` reattach, and it applies per consumer rather than per fleet.

**A better visibility surface for this shape.** Consumers are top-level executions in their own ID
space, so you query them directly instead of listing workflows and describing each one:

```bash
temporal activity list --query "ActivityType = 'Consume' AND Status = 'Running'"
temporal activity describe  --activity-id kafka-consumer-temporal-activity-consumer-0
temporal activity terminate --activity-id kafka-consumer-temporal-activity-consumer-0 --reason "done"
```

**One less concept to explain.** "The workflow is deliberately trivial — it exists only to own the
activities' lifecycle" is a paragraph readers of a reference implementation reasonably stop and
question. It goes away.

### What you give up

**Public Preview, and the version floor is real.** Requires Temporal Server 1.31+ and Java SDK
1.35.0+; this repo pins `temporal.version` at `1.31.0`, so it is an SDK bump, not just new code.
`ActivityClient`, `StartActivityOptions`, and `ActivityHandle` are `@Experimental`. `temporal server
start-dev` enables the feature by default, but a self-hosted cluster needs the
`activity.enableStandalone` dynamic config flag, which ships **disabled**.

**Pause is not available.** Pause, reset, and update options are deferred to GA. Pausing consumption
— stop polling, hold offsets, resume — is exactly what you want during a downstream outage, and today
you can at least model it as a signal on the workflow. `TerminateExisting` is also unsupported, so
"restart this consumer with new settings" is terminate-then-start rather than one atomic call.

**You lose the group handle.** One workflow ID is currently *the* handle for the entire fleet: one
terminate stops everything, and once the workflow starts, Temporal guarantees all N activities get
scheduled. Standalone means the client issues N starts, and partial failure — three of five started,
then the pod dies — becomes your bootstrap code's problem to reconcile.

**There is nowhere left to put coordination.** Rescaling on a signal, staggered starts, reacting when
one consumer fails terminally, rotating topics: all of it has an obvious home in the workflow today.
Without one, that logic moves into the launching application — which is the "one more thing to
operate" that this pattern exists to avoid.

**Spring Boot integration is uncharted.** The starter has no `ActivityClient` autoconfiguration, and
the worker in this module exists as a side effect of `@WorkflowImpl`. Check `TestWorkflowEnvironment`
support for standalone starts before rewriting `ConsumerActivityContextTest`.

### ⚠️ What does not change

Readers assume these improve. They do not:

- **The execution slot trap is identical.** Standalone activities occupy the same worker activity
  slots. `max-concurrent-activity-executors` headroom is still mandatory.
- **Killing the process still does not stop the consumer.** The execution is durable server-side
  either way. Only the command changes — `temporal activity terminate` instead of `workflow terminate`.
- **Cost and per-message visibility.** Still 1 Action per message plus throttled heartbeats. Dropping
  the workflow saves one start and a couple of workflow tasks for the life of the consumer, which
  rounds to zero. Still nothing per-message in history; that is still Pattern 2.
- **Kafka's committed offsets remain the source of truth for recovery.**

### Recommendation

Do not migrate Pattern 3 yet. This repo's value is that all three patterns run out of the box, and
a Public Preview dependency with no pause support is not worth that. Revisit when Standalone
Activities reach GA — the argument for promoting 3b to the default is strong, and because the
activity code carries over unchanged, the migration stays confined to `ConsumerBootstrap`.
