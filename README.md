# subflow

A minimal, open-source-friendly Go playground that models a subscription lifecycle on top of [Temporal](https://temporal.io). Designed to demonstrate Temporal's entity-workflow pattern, signal/query/update primitives, custom search attributes, and visibility-driven listing — all without a side-DB projection.

> **Status:** Learning POC. Not production-ready. Deliberately minimal.

## Lifecycle at a glance

```
                        (no trial)
        start ────────────────────────▶ active ────────cancel signal────▶ canceled
          │                              │  ▲                                │
          │ (trial)                      │  │ recover                        │ period end
          ▼                              ▼  │                                ▼
        trialing ──trial ends, paid──▶ active                            deactivated
          │                              │
          │ cancel / no card             │ renewal payment fails
          ▼                              ▼
        deactivated                    past_due ──retries exhausted──▶ deactivated
                                         │
                                         └────── retry succeeds ──▶ active
```

5 phases. 10 optional notification hooks. Continue-As-New per billing period.

## Quickstart

```bash
git clone https://github.com/martavoi/subflow
cd subflow
task up                           # podman compose up -d --build
open http://localhost:8233        # Temporal Web UI

task seed-plan-monthly            # create a 30s-period plan
task list-plans                   # find the plan ID

task create-subscription USER=alice PLAN_ID=<plan-id>
task list-subscriptions PHASE=active
task list-billing-events SUB_ID=<sub-id>

# Watch each billing period (every 30s) in the Web UI:
#   - new workflow run starts (Continue-As-New from previous)
#   - charge activity runs, billing event written
#   - payment.succeeded + subscription.renewed hooks fire (if integration is up)

# Stop the mock to demo retries:
task break-integration
# ... wait ~30s, see retries pile up in the Web UI
task fix-integration

# Cancel:
task cancel-subscription ID=<sub-id>
# Subscription remains active until the current period ends, then deactivates.
```

> If host port `50051` is taken (e.g., another gRPC service is already running), set `API_HOST_PORT=50053` and update `Taskfile.yml`'s `API_HOST` to match.

## How CreateSubscription works

`CreateSubscription` is **synchronous for non-trial plans** and **immediate for trial plans**:

- Non-trial plan: the gRPC call blocks while the workflow charges the card and runs activation hooks. Returns `phase: active` or `FailedPrecondition` (with the failure reason) on decline.
- Trial plan: the gRPC call returns immediately with `phase: trialing`. The first charge runs at trial end.

Internally both paths use Temporal's `UpdateWithStartWorkflow` to start the workflow and deliver the activation update in one round trip. The workflow itself is one execution per subscription, parked between billing periods on a durable timer.

## What replaces the projection

Listing/filtering is powered entirely by Temporal Visibility. Custom search attributes (`SubflowUserId`, `SubflowPlanCode`, `SubflowPhase`, `SubflowPeriodEnd`, `SubflowTrialEnd`) let you run queries like:

```sql
SubflowUserId='alice' AND SubflowPhase='active'
SubflowPlanCode='premium' AND SubflowPeriodEnd < '2026-06-01T00:00:00Z'
SubflowPhase IN ('past_due','canceled')
```

`ListSubscriptions` builds these from request fields and calls `client.ListWorkflow`. Per-user limit enforcement uses `client.CountWorkflow`. The workflow itself owns the live state, queryable on demand. No projection collection.

## Billing history

Each charge attempt (success or failure, including dunning retries) is written to an append-only Mongo collection `billing_events` by the `RecordBillingEvent` activity. Read via `ListBillingEvents` — paginated, filterable by subscription, user, or type. The `EventStore` interface lets you swap Mongo for Kafka if you need fan-out to multiple consumers.

## Hooks

10 optional notification hooks. Plans declare which they want via `enabled_hooks`. Each hook is a separate gRPC method on the integration service:

| Hook | Fires when |
|---|---|
| `subscription.trial_started` | Workflow enters trialing |
| `subscription.trial_will_end` | `TrialEndNoticeBefore` before trial ends |
| `subscription.activated` | First payment succeeded |
| `subscription.renewed` | Nth payment succeeded (N ≥ 1) |
| `subscription.past_due` | Renewal charge failed, entering dunning |
| `subscription.recovered` | Dunning retry succeeded |
| `subscription.canceled` | Cancel signal received |
| `subscription.deactivated` | Workflow ending (terminal) |
| `payment.succeeded` | Each successful charge |
| `payment.failed` | Each failed charge attempt |

All hooks are **fire-and-forget** — the workflow does not branch on the response. Integrators that need to mutate subscription state call `UpdateSubscriptionContext` on subflow's gRPC API.

## Architecture diagram

See [docs/architecture.md](docs/architecture.md). Concept-to-Temporal cheat sheet: [docs/temporal-cheatsheet.md](docs/temporal-cheatsheet.md).

## Stack

- Go 1.23
- Temporal Go SDK + Temporal dev server (SQLite-backed, embedded Web UI with custom SAs)
- Mongo 7 for plans + append-only billing_events
- gRPC + buf
- Podman / Docker compose

## Failure injection

`mock-integration` env vars (set via compose.yml or `.env`):

| Var | Effect |
|---|---|
| `FAILURE_RATE` | Hook returns gRPC Unavailable (retryable) |
| `TERMINAL_FAILURE_RATE` | Hook returns gRPC FailedPrecondition (non-retryable) |
| `LATENCY_MS` | Artificial latency per call |

Worker also has `PAYMENT_TRANSIENT_RATE` and `PAYMENT_TERMINAL_RATE` for the `ChargePayment` activity.

## Roadmap (out of POC)

- Plan upgrade/downgrade (mid-period changes)
- Pause / resume
- Refunds + `payment.refunded` hook
- Coupons / promotional pricing + proration
- Kafka-backed `EventStore` for fan-out
- Workflow versioning helpers (`workflow.GetVersion`)
- Production-grade Temporal deployment (Elasticsearch visibility, Cassandra)

## License

MIT — see [LICENSE](LICENSE).
