# producer

Publishes `OrderCompleted` events to `orders.completed` for the three consumption patterns to read.

Knows nothing about Temporal — it shares only the event model via `common`, and the Temporal Spring
Boot starter is deliberately off its classpath.

```bash
mvn -f producer/pom.xml spring-boot:run     # http://localhost:8080
```

## API

| Call | Effect |
| :- | :- |
| `POST /orders` | Publish one event. Send an `OrderCompleted` body to control it, or omit to generate one. |
| `POST /orders/batch?count=N` | Publish N events. |
| `POST /orders/stream?ratePerSecond=R` | Start continuous generation. |
| `DELETE /orders/stream` | Stop it. |
| `GET /orders/stream` | Current rate, total published, topic, partitions, injection rates. |
| `POST /orders/injection?duplicateRate=&malformedRate=` | Change injection rates without restarting. |

```bash
curl -sX POST localhost:8080/orders | jq
# {"orderId":"ORD-000001","partition":1,"offset":0,"duplicate":false,"malformed":false}

curl -sX POST "localhost:8080/orders/stream?ratePerSecond=50" | jq
curl -sX DELETE localhost:8080/orders/stream | jq
```

## Configuration

| Property | Default | Meaning |
| :- | :- | :- |
| `app.producer.topic` | `orders.completed` | Destination topic (created on startup). |
| `app.producer.partitions` | `6` | **The throughput ceiling every consumption pattern shares.** Raise this, not the consumer count, for more parallelism. |
| `app.producer.seed` | `20260812` | Generator seed — same seed reproduces the same orders. |
| `app.producer.order-id-prefix` | *(derived from start time)* | Namespaces order IDs per run. See below. |
| `app.producer.duplicate-rate` | `0.0` | Fraction of events repeating an earlier `orderId`. |
| `app.producer.malformed-rate` | `0.0` | Fraction of events that are deliberately unparseable. |
| `app.producer.default-rate-per-second` | `10` | Rate used when `/orders/stream` is called without one. |

## Why order IDs are namespaced per run

IDs look like `ORD-{prefix}-000001`, where the prefix defaults to a short token derived from the
producer's start time.

This matters because the order ID determines the workflow ID, and every consumer deduplicates on it.
With a bare counter, restarting the producer would replay `ORD-000001` onward — and each of those
events would be correctly rejected as already handled. The result is a run that consumes messages,
starts zero workflows, and looks completely broken while behaving exactly as designed. It also turns
any load test into a measurement of deduplication rather than throughput.

So fresh IDs are the default, and reproducibility is opt-in:

```yaml
app:
  producer:
    seed: 20260812
    order-id-prefix: fixed-run-1    # set both to replay a run byte for byte
```

Setting the prefix is also how you generate genuine *cross-run* duplicates on purpose — restart the
producer with the same prefix and seed, and every event is a redelivery of the previous run.

## Why the two injection knobs exist

They are there to prove consumer behaviour, not for realism.

**Duplicates** exercise the idempotency path. Every consumer derives a deterministic workflow ID
from the `orderId` and starts with `WorkflowIdConflictPolicy.USE_EXISTING`, so a redelivered message
attaches to the existing execution instead of creating a second one. Turn duplicates up and the
count of workflow executions should not move.

```bash
curl -sX POST "localhost:8080/orders/injection?duplicateRate=0.2"
```

**Malformed records** exercise poison-message handling. A record that cannot be deserialized must be
routed to `orders.completed.DLT` and its offset committed — otherwise it blocks its partition
forever. All three patterns must behave identically here.

```bash
curl -sX POST "localhost:8080/orders/injection?malformedRate=0.05"
```

Malformed records are published through a separate `KafkaTemplate<String, String>`, since by
definition they can't go through the typed serializer.

## Notes

- Events are keyed by `orderId`, so all events for an order land on one partition and are handled in
  order by one consumer.
- The producer is idempotent (`enable.idempotence=true`, `acks=all`).
- Continuous generation publishes on a fixed 100 ms tick with fractional carry-over, rather than
  scheduling one task per event — at a few hundred events/second, per-event scheduling jitter would
  otherwise dominate any measurement taken from it.
