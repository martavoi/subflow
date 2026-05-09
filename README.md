# subflow

A minimal, open-source-friendly Go playground that models a subscription lifecycle on top of [Temporal](https://temporal.io). It demonstrates how durable workflows replace a polling renewal scheduler, signals replace cancel APIs, and activities with retry policies replace ad-hoc retry loops.

> **Status:** Learning POC. Not production-ready. Deliberately minimal.

## Quickstart

```bash
git clone https://github.com/martavoi/subflow
cd subflow
task up                       # podman compose up -d --build
open http://localhost:8233    # Temporal Web UI

task seed-plan                # creates a 30s-period demo plan
task list-plans               # find the plan ID

task create-subscription USER=alice PLAN_ID=<plan-id>
task list-subscriptions

# Watch the renewals stream by in the Temporal Web UI: each billing period is
# a discrete workflow run, chained via Continue-As-New.

task cancel-subscription ID=<sub-id>
# Subscription remains active until the current period ends, then deactivates.
```

> If host port `50051` is taken (e.g., another gRPC service is already running), set `API_HOST_PORT=50053` (or any free port) and update `Taskfile.yml`'s `API_HOST` variable to match.

## Sync vs async semantics

`CreateSubscription` is **synchronous**: the gRPC call blocks while the workflow runs the activation activities (charge → publish event → notify integration → write projection) and returns once the subscription is active. If the customer's card is declined, the call returns `FailedPrecondition` with the failure reason — exactly what a typical billing API would do. Internally this uses Temporal's `UpdateWithStartWorkflow` to start the workflow and run an `Activate` update in a single round trip.

`CancelSubscription` is **fire-and-forget**: it sends a signal to the workflow and returns. The workflow honors end-of-period semantics — the active billing period runs to completion, then deactivation activities run, then the workflow ends.

Renewals run **asynchronously inside the workflow** every billing interval — no synchronous customer is waiting for a renewal, and the workflow's durable timer fires regardless of API uptime.

## What you'll see in the Web UI

1. **A workflow per subscription**, ID-prefixed `subscription:`.
2. **An `Activate` workflow update** at the start of activation — visible as an Update event in the history; the activation activities run inside it.
3. **Chained Continue-As-New runs**, one per billing period — bounded history regardless of subscription duration.
4. **Activity retries with backoff** when you `task break-integration` (mock-integration is unavailable). Restart with `task fix-integration` and watch the queued retries drain.
5. **Cancel-as-signal** semantics: signal arrives mid-period, workflow honors end-of-period, then runs deactivation.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the diagram and component breakdown. Cheat-sheet of subscription concepts mapped to Temporal primitives is in [docs/temporal-cheatsheet.md](docs/temporal-cheatsheet.md).

## Stack

- Go 1.23
- Temporal Go SDK + Temporal dev server (SQLite-backed, embedded Web UI)
- Mongo 7 for plans + subscription read-model
- gRPC + buf
- Podman / Docker compose

## Failure injection

`mock-integration` honors three env vars (set via compose.yml or `.env`):

| Var | Effect |
|---|---|
| `FAILURE_RATE` | Probability of returning gRPC `Unavailable` (retryable) |
| `TERMINAL_FAILURE_RATE` | Probability of returning gRPC `FailedPrecondition` (non-retryable; will fail the workflow) |
| `LATENCY_MS` | Artificial latency per call |

The worker also has `PAYMENT_TRANSIENT_RATE` and `PAYMENT_TERMINAL_RATE` for the `ChargePayment` activity.

## Roadmap (out of POC scope)

- Workflow versioning helpers (`workflow.GetVersion`)
- Plan upgrades/downgrades mid-period via signals
- Pause / resume signals
- Replace stdout event publisher with Kafka
- Production-grade Temporal deployment (Cassandra, multi-region)

## License

MIT — see [LICENSE](LICENSE).
