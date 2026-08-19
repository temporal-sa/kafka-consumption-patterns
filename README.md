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

This matters because the common intuition, that an external consumer application scales better than
an in-Temporal approach, is about *familiarity of deployment tooling* rather than throughput.
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
all**, and throughput went slightly *down* because more members were coordinating over the same work.

Note also that scaling below the ceiling is sub-linear (2.93x from 6 consumers, not 6x). Something
else binds first, whether that is the target workflows, the worker pool, or the namespace's
Actions-per-second limit. Which is the deeper point: **consumption is rarely the part worth
optimizing.**

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
cycle, which is what this repo is fixed at. See [Action cost](#action-cost) below.

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

**Do not paste this block into a single shell.** Steps 1 and 3 run to completion and hand the prompt
back. Steps 2, 4, 5, and 6 each hold the foreground until you stop them, so every one of those needs
its own terminal. Pasted into one shell, step 2 blocks and nothing after it ever starts, which looks
exactly like the worker and consumers having been left out. Running all three consumers means six
long-running terminals, plus whichever shell you used for steps 1 and 3.

```bash
# 1. Start Kafka (KRaft, no ZooKeeper) + Kafka UI. Detached, so the prompt comes back.
docker compose up -d

# 2. IN A NEW TERMINAL: start the Temporal dev server. Web UI at http://localhost:8233.
temporal server start-dev

# 3. Build once so modules resolve the parent POM. This one finishes and returns.
mvn -DskipTests install

# 4. IN A NEW TERMINAL: run the worker that executes OrderEmailWorkflow.
mvn -f order-email-worker/pom.xml spring-boot:run

# 5. IN A NEW TERMINAL: run the producer.
mvn -f producer/pom.xml spring-boot:run

# 6. IN A NEW TERMINAL EACH: run any or all of the three consumers.
mvn -f consumer-external/pom.xml spring-boot:run     # Pattern 1
mvn -f consumer-workflow/pom.xml spring-boot:run     # Pattern 2
mvn -f consumer-activity/pom.xml spring-boot:run     # Pattern 3
```

Each pattern uses its own consumer group, so running all three means every event is consumed three
times, once per pattern, producing three workflow executions with distinguishable IDs
(`order-email-ext-…`, `order-email-wf-…`, `order-email-act-…`). That side-by-side run is the point of
the repo.

To run this against Temporal Cloud instead of the dev server, step 2 goes away and steps 4 through 6
each gain a profile and a set of credentials. See [Temporal Cloud](#temporal-cloud) — all four of
those processes need them, not just the worker.

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
# Start a workflow by hand (no Kafka involved)
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
# Compare what each pattern has done. Same tags, so they graph on one chart
for p in 8082 8083 8084; do
  curl -s localhost:$p/actuator/prometheus | grep -E "^(kafka_messages_consumed|kafka_records_dlt|temporal_workflows)"
done

# Pattern 2 also exposes its loop state, including history growth
curl -s localhost:8083/consumer | jq
```

Run all three against the same backlog and the totals should agree exactly: same messages consumed,
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

Single instance, 6-partition topic, everything on one laptop. Each case holds a fixed rate for 40 to
45 seconds.

| Pattern | 10/s | 50/s | 100/s | p50 latency at 50/s |
| :- | :- | :- | :- | :- |
| 1 — External App | 10.3 ✓ | 51.6 ✓ | 93–103 ~ | 0.011 s |
| 2 — Workflow *(batch 1)* | **3.4 ✗** | **3.4 ✗** | **3.4 ✗** | **24.7 s** |
| 3 — Long-Running Activity | 10.3 ✓ | 51.5 ✓ | 99–107 ~ | 0.011 s |

✓ = kept up (lag flat). ✗ = fell behind, so the number is a ceiling rather than a rate. ~ = at the
saturation edge, keeping up in most runs but not all.

The 100/s column is a range over six runs rather than one measurement, because a single run is not
enough at that rate. Pattern 1 landed between 92.7 and 103.0 msg/s and kept up in four of the six.
Pattern 3 landed between 99.4 and 106.6 and kept up in five. One of the six degraded both patterns
together, 6.1 s and 6.4 s at p50 against tens of milliseconds everywhere else, which is the laptop
rather than either consumer. **Do not use this column to rank patterns 1 and 3.** Their ranges
overlap, and the spread within each is wider than the distance between them.

There is deliberately no column above 100/s. The producer in this repo tops out near **157 events/s**
on this hardware, so higher rungs measure the producer instead of the consumers. That is
measured rather than inferred: a case requesting 400/s delivered 157.3/s, and a ladder run out to 250
and 500 events/s offered the same load at both. `scripts/load-test.sh` now reports delivered rate
against requested rate per case and warns when they diverge, so this is visible at the point of
measurement. An earlier version of this table did publish a
150/s column, and it disagreed with itself by **1.95×** between two runs on one afternoon, which is
what a rate sitting just under the producer's ceiling produces. The batch-size result below is 15×,
comfortably outside all of this.

**The result that should change how you read the trade-off table:** Pattern 2 at one record per poll
is pinned near **3.4 msg/s no matter what you offer it**, so it cannot keep up with even 10 events/s.
Each message costs three *serialized* activity round-trips (poll → start → commit), which puts a
hard floor on the loop that has nothing to do with Kafka or the target workflow.

Raise `batch-size` to 50 and the same pattern does 51.7 msg/s with 0.57 s p50, a gain of **15× on
throughput and 45× on latency**. The reference architecture compares the patterns on Actions per message and says
nothing about throughput; on that measure batch-size 1 merely looks expensive. It is also unusable
above trivial volumes. See `consumer-workflow/README.md`.

### Scaling out

Adding consumers works, sub-linearly and mostly by cutting latency. At 150/s offered:

| Pattern | 1 consumer | 3 consumers | gain | p50 at 3 |
| :- | :- | :- | :- | :- |
| 1 — External App | 60.7 | 129.5 | 2.13× | 5.03 s (from 22.3 s) |
| 3 — Long-Running Activity | 70.9 | 148.9 | 2.10× | 0.84 s (from 14.5 s) |

Tripling consumers roughly doubled throughput for both. The latency improvement is the more
dramatic effect, with Pattern 3 going from 14.5 s to under a second at p50. And this only works up to
the partition count; see the ceiling demo above.

Three caveats worth stating before quoting any of this. These numbers are one laptop with a
dev-server Temporal and a single broker, which makes them useful for comparing the patterns under
identical conditions and useless as absolute capacity. `RESOURCE_EXHAUSTED` was zero throughout and
worker CPU never passed 5%, so neither the namespace limit nor the shared worker was the constraint.
And both columns have a problem, in opposite directions, so read the gains as lower bounds. Pattern
3's 148.9 is within 5% of the producer's own ~157/s ceiling, so that run was probably starved and its
real capacity is higher. The single-consumer figures are one draw from a distribution that spans
1.95× at this rate. Neither error is large enough to turn 2.1× into 3×, which is the only claim this
table is making.

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
*poll cycle* (scheduling poll, start, and commit), however many records that cycle returned. So
unbatched it costs roughly 4× the others, and batched it is within a few percent of them.

Note this differs from the reference architecture's published "3 actions per message" for the
Workflow pattern, which counts the three activity schedules but omits the workflow start that the
External Client column does count. The two columns were measuring different things.

**Pattern 2's poll batch size is fixed at 1** (`DEFAULT_BATCH_SIZE`), so the implementation
validates the 3-Actions/message figure in the reference architecture rather than diverging from it.
Batching would amortize the loop cost across the batch, which at 50 records per poll works out to
roughly 0.06 Actions/message. If that constant is ever raised, **the cost row in the reference architecture doc
must be updated to match.**

---

## Configuration

Property names are consistent across modules.

| Property | Env var | Default | Meaning |
| :- | :- | :- | :- |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker list |
| `app.producer.topic` | `APP_PRODUCER_TOPIC` | `orders.completed` | Source topic |
| `app.producer.partitions` | `APP_PRODUCER_PARTITIONS` | `6` | **The shared throughput ceiling** |
| `app.producer.seed` | `APP_PRODUCER_SEED` | `20260812` | Generator seed, for reproducible runs |
| `app.producer.order-id-prefix` | `APP_PRODUCER_ORDER_ID_PREFIX` | *(start-time token)* | Namespaces order IDs per run; set it with `seed` to replay a run exactly |
| `app.producer.duplicate-rate` | `APP_PRODUCER_DUPLICATE_RATE` | `0.0` | Fraction of events repeating an earlier `orderId` |
| `app.producer.malformed-rate` | `APP_PRODUCER_MALFORMED_RATE` | `0.0` | Fraction of events that are unparseable |
| `spring.temporal.connection.target` | `TEMPORAL_ADDRESS` | `local` | `local`, or `host:port` |
| `spring.temporal.namespace` | `TEMPORAL_NAMESPACE` | `default` | Namespace |

Two mechanisms are at work in that column, and the difference matters if you go looking for the
names in the source. `KAFKA_BOOTSTRAP_SERVERS`, `TEMPORAL_ADDRESS`, and `TEMPORAL_NAMESPACE` appear
literally in each `application.yml` as placeholders. The `APP_*` names appear nowhere, because they
come from Spring Boot's relaxed binding, which maps any `@ConfigurationProperties` key to its
uppercase underscore-separated form. Both are overrides on the same properties, so either style
works for any row here. The scripts already lean on the second one, setting
`APP_CONSUMER_PARALLEL_CONSUMERS` and `APP_CONSUMER_GROUP_ID` to vary consumers between runs.

### Consumer settings

All three consumer modules bind the same `app.consumer` prefix, but each exposes a different set of
fields. Setting one pattern's property on another pattern is silently ignored rather than rejected,
so check the Pattern column before copying a command between modules.

| Property | Env var | Default | Pattern | Meaning |
| :- | :- | :- | :- | :- |
| `server.port` | `SERVER_PORT` | `8082` / `8083` / `8084` | 1, 2, 3 | HTTP port. Must be distinct to run two instances on one machine |
| `app.consumer.instance-id` | `CONSUMER_INSTANCE_ID` | `1` | 2, 3 | Task queue, workflow ID, and consumer handle key all derive from it. Must be distinct per instance |
| `app.consumer.topic` | `APP_CONSUMER_TOPIC` | `orders.completed` | 1, 2, 3 | Source topic |
| `app.consumer.dlt-topic` | `APP_CONSUMER_DLT_TOPIC` | `orders.completed.DLT` | 1, 2, 3 | Dead-letter topic |
| `app.consumer.group-id` | `APP_CONSUMER_GROUP_ID` | *(one per pattern)* | 1, 2, 3 | `temporal-external-app`, `temporal-workflow-consumer`, `temporal-activity-consumer`. Distinct so each pattern sees every event |
| `app.consumer.concurrency` | `APP_CONSUMER_CONCURRENCY` | `3` | 1 | Listener threads in this instance |
| `app.consumer.max-start-attempts` | `APP_CONSUMER_MAX_START_ATTEMPTS` | `10` | 1 | Attempts before a record is routed to the DLT |
| `app.consumer.poll-timeout-ms` | `APP_CONSUMER_POLL_TIMEOUT_MS` | `5000` | 2, 3 | How long each poll blocks. For Pattern 2 this also sets the idle Action cost |
| `app.consumer.batch-size` | `APP_CONSUMER_BATCH_SIZE` | `1` | 2 | Records per poll. Pinned at 1; see [Action cost](#action-cost) before changing it |
| `app.consumer.max-records-per-poll` | `APP_CONSUMER_MAX_RECORDS_PER_POLL` | `50` | 3 | Records per poll |
| `app.consumer.parallel-consumers` | `APP_CONSUMER_PARALLEL_CONSUMERS` | `2` | 3 | Consume activities running in parallel. Must stay well below the worker's activity slots |

Scaling out is a matter of starting another process with these set. Patterns 2 and 3 need a distinct
`CONSUMER_INSTANCE_ID`, because that is what gives the instance its own task queue:

```bash
# A second Pattern 1 consumer. It joins the existing group and splits partitions with the first.
SERVER_PORT=8092 mvn -f consumer-external/pom.xml spring-boot:run

# A second Pattern 2 consumer, with its own task queue and workflow.
CONSUMER_INSTANCE_ID=2 SERVER_PORT=8093 mvn -f consumer-workflow/pom.xml spring-boot:run
```

Every one of these is capped by the partition count, as the demo at the top of this README shows.
`scripts/load-test.sh` and `scripts/demo-partition-ceiling.sh` drive consumers exactly this way if
you want worked examples.

### Temporal Cloud

**Four of the five apps hold a Temporal connection, and every one of them needs the same credentials.**
The producer is the only exception: it publishes to Kafka and never talks to Temporal at all.

| Component | What it does on the connection | Needs Cloud config | Ships the `cloud` / `cloud-mtls` profiles |
| :- | :- | :- | :- |
| `order-email-worker` | Worker — executes `OrderEmailWorkflow` and its activities | Yes | ✅ |
| `consumer-external` | Client only — starts workflows, executes none | Yes | ✅ (plus a `SASL_SSL` Kafka block that is inert — see below) |
| `consumer-workflow` | Client **and** worker — hosts the consumer workflow and its poll/start/commit activities | Yes | ✅ |
| `consumer-activity` | Client **and** worker — hosts the long-running consume activity | Yes | ✅ |
| `producer` | Nothing. Kafka only | No | n/a |
| `scripts/*.sh` | `temporal` CLI — lists and terminates consumer workflows between runs | Yes | n/a, env vars only |

Each template offers two auth mechanisms, and **which one you get is a matter of which profiles are
active**, not of editing YAML. One set of exports covers every component.

**Option A, API key** — the `cloud` profile alone:

```bash
export TEMPORAL_ADDRESS=my-namespace.a1b2c.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=my-namespace.a1b2c
export TEMPORAL_API_KEY=...
export SPRING_PROFILES_ACTIVE=cloud
```

**Option B, mTLS** — `cloud` *plus* `cloud-mtls`. Both profiles, and leave `TEMPORAL_API_KEY` unset so
no auth header is added on top of the certificate:

```bash
export TEMPORAL_ADDRESS=my-namespace.a1b2c.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=my-namespace.a1b2c
export TEMPORAL_TLS_CLIENT_CERT_PATH=/path/client.pem
export TEMPORAL_TLS_CLIENT_KEY_PATH=/path/client.key
export SPRING_PROFILES_ACTIVE=cloud,cloud-mtls
```

The key must be **PKCS8** (`-----BEGIN PRIVATE KEY-----`). Convert a PKCS1 key
(`-----BEGIN RSA PRIVATE KEY-----`) with `openssl pkcs8 -topk8 -nocrypt -in old.key -out new.key`, or
for a `.p12`/`.pfx` bundle set `mtls.pkcs: 12` in the profile and supply `key-file` alone. Get the
path wrong and startup fails immediately with `NoSuchFileException` naming the variable, which is
deliberate: a credential that half-applies is worse than one that refuses to start.

With the exports in place, start each Temporal-connected process in its own terminal exactly as in
the [Quickstart](#quickstart). There is no `temporal server start-dev` step:

```bash
mvn -f order-email-worker/pom.xml spring-boot:run
mvn -f consumer-external/pom.xml  spring-boot:run     # Pattern 1
mvn -f consumer-workflow/pom.xml  spring-boot:run     # Pattern 2
mvn -f consumer-activity/pom.xml  spring-boot:run     # Pattern 3

# The producer holds no Temporal connection, so the profile is irrelevant to it.
mvn -f producer/pom.xml spring-boot:run
```

`SPRING_PROFILES_ACTIVE` is used above in preference to `-Dspring-boot.run.profiles=cloud` on each
command for two reasons: it cannot be forgotten in one of four terminals, and it is the only form
that reaches `scripts/load-test.sh`, which starts consumers with `java -jar` — those inherit the
environment but never see the Maven flag.

**These profiles force TLS on** (`enable-https: true`), whichever mechanism you use, and that is
load-bearing. Left to itself, an unset `TEMPORAL_API_KEY` resolves to empty, the SDK sees no
credential of any kind, and the channel falls back to **plaintext** — against a TLS-only Cloud
endpoint that surfaces as a connection reset, which reads like a network fault rather than a missing
credential. Forcing TLS on turns that into an auth error at the first call instead. If you point the
cloud profile at a self-hosted Temporal with no TLS, override it with
`SPRING_TEMPORAL_CONNECTION_ENABLE_HTTPS=false`.

Anything in these profiles can also be supplied as a bare environment variable, no profile involved,
because Spring's relaxed binding maps `SPRING_TEMPORAL_CONNECTION_API_KEY` and
`SPRING_TEMPORAL_CONNECTION_MTLS_KEY_FILE` onto the same properties on top of the default
`application.yml`. Useful for a one-off override; the profiles are the maintained path.

**Forgetting the profile is the quiet failure; forgetting a variable is the loud one.** On the cloud
profiles every placeholder is required, so an unset `TEMPORAL_ADDRESS` refuses to start and names
`${TEMPORAL_ADDRESS}` in the error. Without a profile active, though, the base `application.yml`
applies its own default of `local`, and that literal short-circuits the connection entirely: the app
talks plaintext to `127.0.0.1:7233` and ignores every credential in your environment. Exporting the
whole Cloud set but omitting `SPRING_PROFILES_ACTIVE` therefore runs happily against a dev server
that may not even be there, which is the one case here that fails silently rather than loudly.

**Kafka is a separate question, and a secured broker is not solved here.** Every module builds its
Kafka client config in code from `spring.kafka.bootstrap-servers` alone — see `KafkaConsumerConfig`,
`producer`'s `KafkaConfig`, and `common/kafka/KafkaConsumers`. None of them read
`spring.kafka.properties`, which means the `SASL_SSL` block in `consumer-external`'s cloud profile
does not currently reach its consumer factory or its DLT template. Pointing this repo at a managed
Kafka needs those client configs to merge Spring's `KafkaProperties`, which is a code change rather
than a YAML one. The scripts under `scripts/` are further still from it: they require the *local*
Docker broker no matter where Temporal runs, because they drive `kafka-topics.sh` and
`kafka-consumer-groups.sh` through `docker exec kafka`.

These variable names match Temporal's
[environment configuration](https://docs.temporal.io/develop/environment-configuration) convention,
so one set of exports serves the CLI, the scripts under `scripts/`, and every app here. Note that
the apps read them through Spring's own placeholder resolution rather than through the SDK's
`temporal-envconfig` module, so a `temporal.toml` profile is not consulted. Only the environment
variables are.

The profile is declarative only. No code paths differ between local and cloud, so it cannot drift
behaviorally from what the test suite covers locally.

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
`temporal_workflows_started_total` is flat, the events carry order IDs that were already processed,
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
HTTP 400, and that rejection surfaces as this misleading message. The parent POM pins
`docker.api.version` (default
`1.43`) to avoid it. On an older daemon, override it:

```bash
mvn verify -Ddocker.api.version=1.41
```

**Build fails immediately with a release-version error.** The project targets Java 21; check
`java -version` and point `JAVA_HOME` at a JDK 21 or newer.

**`Could not find artifact io.temporal.samples:kafka-consumption:pom`** when running a single
module. Run `mvn -DskipTests install` once from the repo root so modules can resolve the parent POM.

**Workflows stay in retry for up to 30s after `chaos/email/up`.** Expected, because retry backoff
caps at 30 seconds, so recovery is not instant.

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

What is *not* shared is the thing being compared: where the consumer loop lives, and what that costs
in visibility, Actions, and operational surface.

The worker is deliberately separate from the consumers: all three patterns start the *same* workflow
on the *same* task queue, so any difference you observe between them comes from the consumption
approach alone.
