# Temporal + Kafka: Three Consumption Patterns (Java / Spring Boot)

A runnable reference implementation of the three ways to consume Kafka messages and start Temporal
Workflows, built so they can be read side by side, run side by side, and measured.

Design spec: [PRD.md](PRD.md).

---

## Read this before comparing the patterns

> **All three patterns share one throughput ceiling: the number of partitions on the topic.**
>
> Kafka assigns each partition to at most one consumer in a group. A fleet of external client
> applications, a fleet of consumer workflows, and a fleet of long-running activities are bounded
> **identically**. Adding a 7th consumer of any kind to a 6-partition topic gives you an idle
> consumer, not more throughput.

This matters because the common intuition — that an external consumer application scales better
than an in-Temporal approach — is about *familiarity of deployment tooling*, not about throughput.
If consumption is your bottleneck, add partitions. The pattern you pick will not move that ceiling.

Choose a pattern on **visibility, Action cost, and what you want to operate** instead.

### Don't take our word for it

```bash
./scripts/demo-partition-ceiling.sh 1 3 6 9
```

Measured on a 6-partition topic, draining a 3,000-message backlog:

```
CONSUMERS   MEMBERS   IDLE    DRAIN(s)   RATE(msg/s)   vs BASE
1           1         0       39.9       75.3          1.00x
3           3         0       17.4       172.7         2.29x
6           6         0       13.6       221.0         2.93x
9           9         3       14.3       209.4         2.78x
```

The knee sits exactly on the partition count. At 9 consumers, **3 were assigned no partitions at
all** and throughput went slightly *down* — more members coordinating over the same work.

Note also that scaling below the ceiling is sub-linear (2.93x from 6 consumers, not 6x). Something
else binds first — the target workflows, the worker pool, or the namespace's Actions-per-second
limit. Which is the deeper point: **consumption is rarely the part worth optimising.**

Where a real difference does emerge is *downstream* of consumption: every pattern starts the same
`OrderEmailWorkflow`, and that work scales on its own task queue and worker pool with no partition
limit. Consumption is rarely the part worth optimizing.

---

## The three patterns

| # | Pattern | Consumer lives in | Visibility | Actions/msg | Module |
| :- | :- | :- | :- | :- | :- |
| 1 | External Application | Plain Spring Boot app, `@KafkaListener` | None / custom | 1 | `consumer-external` |
| 2 | Workflow | A Temporal Workflow calling poll/start/commit activities | Every message in history | 3 | `consumer-workflow` |
| 3 | Long-Running Activity | One heartbeating activity looping forever | Some (pending activity) | 1 + heartbeats | `consumer-activity` |

Actions/msg excludes the target workflow itself. Pattern 2's **3** assumes one message per poll
cycle, which is what this repo is fixed at — see [Action cost](#action-cost) below.

---

## Current status

**All three patterns are implemented and runnable side by side.**

| Module | Status | What it is |
| :- | :- | :- |
| `common` | ✅ | Event model, target workflow contract, shared start + Kafka helpers, conventions |
| `order-email-worker` | ✅ | Hosts `OrderEmailWorkflow` + activities, with runtime failure injection |
| `producer` | ✅ | Publishes `OrderCompleted` events; rate, duplicate, and malformed injection |
| `consumer-external` | ✅ | Pattern 1 — `@KafkaListener`, manual ack, DLT routing |
| `consumer-workflow` | ✅ | Pattern 2 — workflow loop, continue-as-new, stop signal, status query |
| `consumer-activity` | ✅ | Pattern 3 — long-running heartbeating activity, N in parallel |

Both measurement tools ship: `scripts/demo-partition-ceiling.sh` (PRD FR-X8) and
`scripts/load-test.sh` (PRD §12).

---

## Quickstart

Requires Docker, JDK 21+, Maven, and the [Temporal CLI](https://docs.temporal.io/cli).

```bash
# 1. Kafka (KRaft, no ZooKeeper) + Kafka UI
docker compose up -d

# 2. Temporal dev server — Web UI at http://localhost:8233
temporal server start-dev

# 3. Build once so modules resolve the parent POM
mvn -DskipTests install

# 4. The worker that executes OrderEmailWorkflow
mvn -f order-email-worker/pom.xml spring-boot:run

# 5. The producer
mvn -f producer/pom.xml spring-boot:run

# 6. The three consumers — run any or all of them at once
mvn -f consumer-external/pom.xml spring-boot:run     # Pattern 1
mvn -f consumer-workflow/pom.xml spring-boot:run     # Pattern 2
mvn -f consumer-activity/pom.xml spring-boot:run     # Pattern 3
```

Each pattern uses its own consumer group, so running all three means every event is consumed three
times — once per pattern — producing three workflow executions with distinguishable IDs
(`order-email-ext-…`, `order-email-wf-…`, `order-email-act-…`). That side-by-side run is the point of
the repo.

| Service | URL |
| :- | :- |
| Temporal Web UI | http://localhost:8233 |
| Kafka UI | http://localhost:8085 |
| Producer API | http://localhost:8080 |
| Worker API (demo + chaos) | http://localhost:8081 |
| Pattern 1 consumer | http://localhost:8082 |
| Pattern 2 consumer | http://localhost:8083 |
| Pattern 3 consumer | http://localhost:8084 |

### Prove it works

```bash
# Start a workflow by hand — no Kafka involved
curl -sX POST localhost:8081/demo/order-email | jq

# Publish events to the topic
curl -sX POST localhost:8080/orders | jq
curl -sX POST "localhost:8080/orders/batch?count=25" | jq

# Continuous generation at 20 events/sec
curl -sX POST "localhost:8080/orders/stream?ratePerSecond=20" | jq
curl -sX DELETE localhost:8080/orders/stream | jq
```

Each published event becomes an `OrderEmailWorkflow` execution per running pattern.

```bash
# Compare what each pattern has done — same tags, so they graph on one chart
for p in 8082 8083 8084; do
  curl -s localhost:$p/actuator/prometheus | grep -E "^(kafka_messages_consumed|kafka_records_dlt|temporal_workflows)"
done

# Pattern 2 also exposes its loop state, including history growth
curl -s localhost:8083/consumer | jq
```

Run all three against the same backlog and the totals should agree exactly — same messages consumed,
same workflows started, same records dead-lettered. They did when this was last verified:

```
                consumed   DLT   started   duplicates
Pattern 2 (wf)       278    13       218           60
Pattern 3 (act)      278    13       218           60
```

---

## The durability demo

This is the argument the whole integration exists to make. With the worker running:

```bash
# Take the third-party email provider offline
curl -sX POST localhost:8081/chaos/email/down | jq

# Start a few workflows
for i in 1 2 3; do curl -sX POST localhost:8081/demo/order-email; done
```

Open http://localhost:8233 and look at the running workflows. Each has completed its order lookup,
shipping lookup, and invoice generation, and is now parked retrying `SendEmail` with exponential
backoff. Nothing has failed, nothing is lost, and the full history is inspectable.

```bash
# Bring the provider back
curl -sX POST localhost:8081/chaos/email/up | jq
```

Every parked workflow completes on its own. No intervention, no replay tooling, no dead-letter
triage.

Other knobs:

```bash
# 30% transient failures on the order database
curl -sX POST localhost:8081/chaos/orderDb \
  -H 'Content-Type: application/json' -d '{"failureRate":0.3}' | jq

# 2s of latency on invoice generation
curl -sX POST localhost:8081/chaos/invoice \
  -H 'Content-Type: application/json' -d '{"latencyMs":2000}' | jq

curl -s localhost:8081/chaos | jq          # current state
curl -sX POST localhost:8081/chaos/reset   # clear everything
```

---

## Measured throughput

```bash
./scripts/load-test.sh                          # full rate ladder, all three patterns
PATTERNS=wf RATES=50 BATCH_SIZE=50 ./scripts/load-test.sh
```

Single instance, 6-partition topic, 40s per case, everything on one laptop:

| Pattern | 10/s | 50/s | 150/s | p50 latency at 50/s |
| :- | :- | :- | :- | :- |
| 1 — External App | 10.3 ✓ | 51.6 ✓ | 70–97 ✗ | 0.011 s |
| 2 — Workflow *(batch 1)* | **3.4 ✗** | **3.4 ✗** | **3.4 ✗** | **24.7 s** |
| 3 — Long-Running Activity | 10.3 ✓ | 51.5 ✓ | 138 ✗ | 0.011 s |

✓ = kept up (lag flat). ✗ = fell behind, so the number is a ceiling rather than a rate.

The 150/s column is deliberately imprecise: repeat runs of the *identical* Pattern 1 configuration
measured 70.0 and 97.4 msg/s, about 40% variance. **Do not use these saturation figures to rank
patterns 1 and 3** — on a laptop also running the broker, the worker, and Temporal, differences
under ~2× are noise. The batch-size result below is 15×, comfortably outside that band.

**The result that should change how you read the trade-off table:** Pattern 2 at one record per poll
is pinned near **3.4 msg/s no matter what you offer it** — it cannot keep up with even 10 events/s.
Each message costs three *serialized* activity round-trips (poll → start → commit), which puts a
hard floor on the loop that has nothing to do with Kafka or the target workflow.

Raise `batch-size` to 50 and the same pattern does 51.7 msg/s with 0.57 s p50 — **15× throughput,
45× latency**. The reference architecture compares the patterns on Actions per message and says
nothing about throughput; on that measure batch-size 1 merely looks expensive. It is also unusable
above trivial volumes. See `consumer-workflow/README.md`.

### Scaling out

Adding consumers works — sub-linearly, and mostly by cutting latency. At 150/s offered:

| Pattern | 1 consumer | 3 consumers | gain | p50 at 3 |
| :- | :- | :- | :- | :- |
| 1 — External App | 60.7 | 129.5 | 2.13× | 5.03 s (from 22.3 s) |
| 3 — Long-Running Activity | 70.9 | 148.9 | 2.10× | 0.84 s (from 14.5 s) |

Tripling consumers roughly doubled throughput for both. The latency improvement is the more
dramatic effect — Pattern 3 went from 14.5 s to under a second at p50. And this only works up to
the partition count; see the ceiling demo above.

Two caveats worth stating before quoting any of this: these numbers are one laptop with a dev-server
Temporal and a single broker — useful for comparing the patterns under identical conditions, useless
as absolute capacity. And `RESOURCE_EXHAUSTED` was zero throughout, so nothing here was namespace
rate limiting.

## Action cost

Temporal Cloud namespaces are rate-limited in Actions/second (500/s by default, adjusted from recent
usage). Consumption cost per pattern:

Counting the workflow start plus each activity execution, and excluding Actions consumed *inside*
the target workflow (identical for all three patterns):

| Pattern | Actions per message | Notes |
| :- | :- | :- |
| External Application | 1 | The workflow start |
| Workflow | **1 + 3/batch** | 4 at one record per poll, 1.06 at fifty |
| Long-Running Activity | 1 + throttled heartbeats | Heartbeats are throttled by the SDK |

Every pattern pays 1 Action per message for the workflow start. Pattern 2 additionally pays 3 per
*poll cycle* — scheduling poll, start and commit — however many records that cycle returned. So
unbatched it costs roughly 4× the others, and batched it is within a few percent of them.

Note this differs from the reference architecture's published "3 actions per message" for the
Workflow pattern, which counts the three activity schedules but omits the workflow start that the
External Client column does count. The two columns were measuring different things.

**Pattern 2's poll batch size is fixed at 1** (`DEFAULT_BATCH_SIZE`), so the implementation
validates the 3-Actions/message figure in the reference architecture rather than diverging from it.
Batching would amortize the loop cost across the batch — at 50 records per poll, roughly 0.06
Actions/message. If that constant is ever raised, **the cost row in the reference architecture doc
must be updated to match.**

---

## Configuration

Property names are consistent across modules.

| Property | Default | Meaning |
| :- | :- | :- |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Broker list |
| `app.producer.topic` | `orders.completed` | Source topic |
| `app.producer.partitions` | `6` | **The shared throughput ceiling** |
| `app.producer.seed` | `20260812` | Generator seed, for reproducible runs |
| `app.producer.order-id-prefix` | *(start-time token)* | Namespaces order IDs per run; set it with `seed` to replay a run exactly |
| `app.producer.duplicate-rate` | `0.0` | Fraction of events repeating an earlier `orderId` |
| `app.producer.malformed-rate` | `0.0` | Fraction of events that are unparseable |
| `spring.temporal.connection.target` | `local` | `local`, or `host:port` |
| `spring.temporal.namespace` | `default` | Namespace |

### Temporal Cloud

Both apps ship an `application-cloud.yml` template:

```bash
export TEMPORAL_TARGET=my-namespace.a1b2c.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=my-namespace.a1b2c
export TEMPORAL_API_KEY=...       # or use the mTLS block instead
mvn -f order-email-worker/pom.xml spring-boot:run -Dspring-boot.run.profiles=cloud
```

The profile is declarative only — no code paths differ between local and cloud, so it cannot drift
behaviourally from what the test suite covers locally.

---

## Metrics

Every app exposes `/actuator/prometheus`. No Prometheus or Grafana ships with this repo by design;
point whatever you already run at those endpoints.

Watch for namespace rate limiting:

```
rate(temporal_long_request_failure_total{status_code="RESOURCE_EXHAUSTED"}[$__rate_interval])
rate(temporal_request_failure_total{status_code="RESOURCE_EXHAUSTED"}[$__rate_interval])
```

On Temporal Cloud, also `temporal_cloud_v0_resource_exhausted_error_count`.

---

## Tests

```bash
mvn clean verify
```

Runs locally with no cloud credentials. Workflow logic is tested against Temporal's test environment
with a virtual clock, so retry backoff is skipped and the durability test above runs in
milliseconds.

---

## Troubleshooting

**The consumer reads messages but no workflows appear.**

Check the metrics first:

```bash
curl -s localhost:8082/actuator/prometheus | grep -E "^temporal_workflows"
```

If `temporal_workflows_duplicate_skipped_total` is climbing while
`temporal_workflows_started_total` is flat, the events carry order IDs that were already processed —
so every consumer is correctly deduplicating them into no-ops. This is the system working, not
failing.

The usual cause is a pinned `app.producer.order-id-prefix`. Left unset (the default), each producer
run namespaces its IDs with a fresh start-time token so restarts never replay. Set it only when you
want to replay a run deliberately.

To start clean instead, reset the topic and consumer offsets:

```bash
docker compose down -v && docker compose up -d
```

**`Could not find a valid Docker environment` when running tests, but Docker is clearly running.**

docker-java defaults to Docker Engine API v1.32, which recent Docker Desktop releases reject with
HTTP 400 — reported as this misleading message. The parent POM pins `docker.api.version` (default
`1.43`) to avoid it. On an older daemon, override it:

```bash
mvn verify -Ddocker.api.version=1.41
```

**Build fails immediately with a release-version error.** The project targets Java 21; check
`java -version` and point `JAVA_HOME` at a JDK 21 or newer.

**`Could not find artifact io.temporal.samples:kafka-consumption:pom`** when running a single
module. Run `mvn -DskipTests install` once from the repo root so modules can resolve the parent POM.

**Workflows stay in retry for up to 30s after `chaos/email/up`.** Expected — retry backoff caps at
30 seconds, so recovery is not instant.

**Pattern 3 keeps consuming after you kill its process.** Working as designed, and the most
surprising property of that pattern: the consumer's lifecycle belongs to Temporal, not to the JVM.
Killing the process only stops the heartbeat; the workflow survives and the next worker to poll
`kafka-consumer-activity` resumes its activities. To actually stop it, terminate the workflow:

```bash
temporal workflow terminate --workflow-id kafka-consumer-activity-1
```

Orphaned consumers accumulate silently and eventually exhaust the worker's activity slots (see
`consumer-activity/README.md`). List them with:

```bash
temporal workflow list --query "WorkflowType = 'KafkaConsumerActivityWorkflow' AND ExecutionStatus = 'Running'"
```

## Layout

```
common/                shared model, target workflow contract, OrderEmailStarter, conventions
order-email-worker/    executes OrderEmailWorkflow (its own task queue)
producer/              publishes OrderCompleted events
consumer-external/     Pattern 1 — external application (spring-kafka @KafkaListener)
consumer-workflow/     Pattern 2 — workflow loop (raw KafkaConsumer in activities)
consumer-activity/     Pattern 3 — long-running activity (raw KafkaConsumer, heartbeating)
```

`OrderEmailStarter` in `common` holds the workflow-start and idempotency logic for **all three**
patterns, and `common/kafka/` holds the record decoding and dead-letter routing shared by patterns 2
and 3. Keeping those in one place is what makes the comparison trustworthy: if each module rolled its
own, differences in that plumbing would masquerade as differences between the patterns.

What is *not* shared is the thing being compared — where the consumer loop lives, and what that costs
in visibility, Actions, and operational surface.

The worker is deliberately separate from the consumers: all three patterns start the *same* workflow
on the *same* task queue, so any difference you observe between them comes from the consumption
approach alone.
