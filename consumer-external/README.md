# consumer-external — Pattern 1: External Application

An ordinary Spring Boot Kafka consumer that starts a Temporal Workflow per message.

```bash
mvn -f consumer-external/pom.xml spring-boot:run    # http://localhost:8082
```

Requires Kafka (`docker compose up -d`), a Temporal dev server, and `order-email-worker` running.

## The whole pattern

```java
@KafkaListener(topics = "${app.consumer.topic}", groupId = "${app.consumer.group-id}")
public void onOrderCompleted(ConsumerRecord<String, OrderCompleted> record, Acknowledgment ack) {
  starter.start(record.value());   // hand the work to Temporal
  ack.acknowledge();               // only then commit the offset
}
```

Everything else in this module is configuration around those two lines. That is the honest summary
of this pattern's appeal: it is a normal consumer deployment with one extra call.

**This app is a Temporal client, not a worker.** It never runs workflow or activity code.
`order-email-worker` does that.

## Why the ordering matters

Start first, acknowledge second.

If the workflow start throws, the offset is never committed and the error handler retries the same
record. A Temporal outage therefore *delays* events rather than dropping them — the partition stalls
until Temporal returns or the record is dead-lettered. Acknowledging first would open a window where
a crash between commit and start loses the message permanently.

Kafka gives you at-least-once, so redelivery is normal. Idempotency comes from the deterministic
workflow ID (`order-email-ext-{orderId}`) plus `ALLOW_DUPLICATE_FAILED_ONLY`: a replay after a
successful run is rejected, a replay after a failed run is allowed through to recover. Both
rejections surface as `WorkflowExecutionAlreadyStarted`, which is caught and counted rather than
treated as an error.

`WorkflowClient.start`, not `execute` — blocking until the email is sent would tie consumption
throughput to downstream latency, which is exactly the coupling this integration removes.

## Poison messages

A record that cannot be deserialized must not block its partition forever.

`ErrorHandlingDeserializer` wraps the JSON deserializer so a parse failure becomes a routable record
instead of an exception thrown deep in the poll loop. `DefaultErrorHandler` then sends it to
`orders.completed.DLT` on the same partition and commits past it. Deserialization failures are
registered as non-retryable — retrying them can never succeed, and doing so would burn the backoff
schedule while the partition stalls.

Transient failures (Temporal unreachable) *are* retried, up to `app.consumer.max-start-attempts`
with exponential backoff, before the record is dead-lettered.

```bash
curl -sX POST "localhost:8080/orders/injection?malformedRate=0.05"   # produce some poison
```

One honest caveat: **the DLT is at-least-once too.** If the consumer restarts after publishing a
record to the DLT but before its source offset is committed, that record is redelivered and
dead-lettered again. Expect occasional duplicates on the DLT and make whatever reads it tolerant of
them — the source topic's offset commit and the DLT publish are not one atomic operation, and making
them so would require the Kafka transactions this repo deliberately avoids.

## Scaling

Two axes, both capped by the same ceiling:

| Axis | How |
| :- | :- |
| Threads per instance | `app.consumer.concurrency` |
| Instances | Run more copies in the same consumer group |

`concurrency × instances` above the topic's partition count just produces idle consumers. **The
partition count is the ceiling for this pattern and for the other two equally** — see the root
README. This pattern's advantage is not throughput; it is that scaling it uses deployment tooling
you already have.

## Visibility — the real tradeoff

There is none, by default. The gap between "message read" and "workflow started" is invisible to
Temporal: if this app crashes mid-loop, nothing in Temporal records that the message existed.

The module compensates with metrics and structured logs, which is the "custom" entry in the
trade-off table. If you need Temporal-native per-message visibility, that is what Pattern 2 buys and
what its extra cost pays for.

## Cost

1 Action per message — the workflow start. The cheapest of the three patterns.

## Configuration

| Property | Default | Meaning |
| :- | :- | :- |
| `app.consumer.topic` | `orders.completed` | Source topic |
| `app.consumer.dlt-topic` | `orders.completed.DLT` | Dead-letter destination |
| `app.consumer.group-id` | `temporal-external-app` | Own group, so all three patterns can run at once |
| `app.consumer.concurrency` | `3` | Listener threads in this instance |
| `app.consumer.max-start-attempts` | `10` | Retries before dead-lettering a transient failure |

## Metrics

At `/actuator/prometheus`, all tagged `pattern="ext"` so the three patterns can be compared on one
graph:

- `kafka_messages_consumed_total`
- `temporal_workflows_started_total`
- `temporal_workflows_duplicate_skipped_total`
- `kafka_event_to_workflow_start_seconds` (p50/p95/p99)

## Tests

`ExternalConsumerIntegrationTest` runs the application unmodified against a real Kafka broker
(Testcontainers) and the Temporal starter's embedded test server — no Compose, no credentials, no
fixed ports. Only the workflow implementation is swapped for a recording stand-in, on the worker
side.

It asserts the three claims that matter: a consumed event starts a workflow, three redeliveries of
one event still produce exactly one execution, and a malformed record reaches the DLT while a valid
record behind it is still processed.

Two environment notes, both handled in the build:

- The test pins `apache/kafka:3.8.1` rather than the 3.9.0 Compose uses. Testcontainers 1.21.0
  configures 3.9.0 with a `0.0.0.0` advertised listener, which that broker refuses to start with.
- The parent POM pins the Docker Engine API version (`docker.api.version`, default 1.43). docker-java
  defaults to v1.32, which recent Docker Desktop releases reject with HTTP 400 — reported as
  "Could not find a valid Docker environment" even though Docker is running fine. Override with
  `-Ddocker.api.version=...` on an older daemon.
