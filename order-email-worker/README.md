# order-email-worker

Executes `OrderEmailWorkflow` — the shared target workflow that all three consumption patterns
start. Runs on its own task queue (`order-email`), separate from any consumer.

```bash
mvn -f order-email-worker/pom.xml spring-boot:run    # http://localhost:8081
```

## Why this is a separate application

Consuming from Kafka and executing the resulting workflows are different scaling concerns.
Consumption is capped by the topic's partition count; this worker pool is not. If throughput becomes
a problem, this is usually the side to scale.

Keeping it separate also keeps the comparison honest: all three patterns start the *same* workflow
on the *same* queue with the *same* worker, so differences you observe between them come from the
consumption approach alone.

## The workflow

```
lookupOrder → lookupShippingDetails → generateInvoice → sendEmail
```

Each step is an activity against a simulated downstream service that can be made slow, flaky, or
dead at runtime.

Retry configuration lives in one place (`OrderEmailWorkflowImpl`), using a single activity stub with
per-activity overrides:

| Activity | Start-to-close | Max attempts |
| :- | :- | :- |
| `LookupOrder`, `LookupShippingDetails` | 10s | unlimited |
| `GenerateInvoice` | 2m | 5 |
| `SendEmail` | 30s | unlimited |

`SendEmail` retries forever on purpose. The third-party provider is the component most likely to be
down, and the guarantee worth demonstrating is that the email arrives late rather than never.

## Failure injection

Four simulated services — `orderDb`, `shippingDb`, `invoice`, `email` — each with `down`,
`failureRate`, and `latencyMs`. All are adjustable at runtime.

```bash
curl -s localhost:8081/chaos | jq                    # current state
curl -sX POST localhost:8081/chaos/email/down        # hard outage
curl -sX POST localhost:8081/chaos/email/up
curl -sX POST localhost:8081/chaos/reset

curl -sX POST localhost:8081/chaos/orderDb \
  -H 'Content-Type: application/json' -d '{"failureRate":0.3,"latencyMs":500}' | jq
```

### The demo

```bash
curl -sX POST localhost:8081/chaos/email/down
for i in 1 2 3; do curl -sX POST localhost:8081/demo/order-email; done
```

In the Web UI each workflow has finished its lookups and invoice, and is parked retrying `SendEmail`
with growing backoff:

```
Pending Activities: 1
  Type             SendEmail
  State            Scheduled
  Attempt          5
  MaximumAttempts  0
  LastFailure      {"message":"email is unavailable", ...}
```

Then:

```bash
curl -sX POST localhost:8081/chaos/email/up
```

Every parked workflow completes on its own. Backoff caps at 30s, so recovery takes up to that long
after the provider returns — worth knowing before you demo it live.

## Starting a workflow by hand

```bash
curl -sX POST localhost:8081/demo/order-email | jq
# {"workflowId":"order-email-manual-DEMO-...","runId":"...","orderId":"DEMO-..."}
```

Send an `OrderCompleted` body to control the payload. This endpoint exists so the workflow and the
chaos demo work before any consumer is built, and it stays useful afterwards for isolating whether a
problem is in consumption or in the workflow.

## Notes

- `TemporalConfig` exposes a `DataConverter` bean configured with `JavaTimeModule`. Every module that
  starts or executes these workflows must declare the same bean — the payloads carry `Instant` and
  `LocalDate`, and if the starting side and the executing side disagree on Jackson configuration,
  the failure surfaces at execution time rather than at start time.
- Workflow code uses `Workflow.currentTimeMillis()`, never `Instant.now()`, so replay is
  deterministic.
- Workers are created by the starter's auto-discovery from `@WorkflowImpl` / `@ActivityImpl`.
