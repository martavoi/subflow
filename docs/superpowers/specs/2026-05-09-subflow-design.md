# subflow — design spec

**Date:** 2026-05-09
**Status:** Draft, pending user approval
**Goal:** A minimal, open-source-friendly Go playground that models a subscription lifecycle on top of Temporal, replacing a polling renewal scheduler with durable workflows. Used to learn Temporal idioms (workflows, activities, signals, queries, retries, Continue-As-New) and to inform a future refactor of an existing subscription service.

---

## 1. Background and goals

### What we are modelling

The subscription lifecycle has three primary phases:

1. **Activation** — subscription is created and "turned on" for a user.
2. **Renewal** — recurring at the end of each billing interval; payment + integration call + event publish.
3. **Deactivation** — terminal phase, run when the subscription is cancelled (at end-of-period) or otherwise expires.

Cancellation is end-of-period: a cancel marks the subscription for non-renewal; the active period runs to completion and then deactivates.

Each subscription owns a mutable `context` (`map[string]string`) that integration services read and write across actions.

### What this POC must demonstrate

| Concern in the existing system | Temporal mechanism |
|---|---|
| Background polling scheduler that scans for due renewals | A durable workflow timer — `workflow.Sleep` until period end |
| gRPC create/cancel/get-status API | Temporal `StartWorkflow`, `SignalWorkflow`, `QueryWorkflow` |
| Event publishing to an external bus (with retries) | Activity with `RetryPolicy` |
| External integration service callouts (with idempotency) | Activity calling a gRPC service, idempotency token from workflow/run/activity IDs |
| Mutable subscription context across actions | Workflow state, persisted across activities, carried through Continue-As-New |

### Non-goals

- Real payment processing — `ChargePayment` is mocked.
- Real event bus — `PublishSubscriptionEvent` writes to stdout in the POC.
- Authentication, authorization, multi-tenancy.
- Plan upgrades/downgrades mid-period — explicitly out of scope; clean extension point.
- Workflow versioning — out of scope; documented as a real-system concern in the README.

---

## 2. Architecture

```
┌─────────────────┐    gRPC    ┌──────────────────────┐
│  client (CLI/   │──────────▶│  subflow-api         │
│  grpcurl)       │            │  (gRPC :50051,       │
└─────────────────┘            │   Temporal client)   │
                               └──────────┬───────────┘
                                          │ StartWorkflow / SignalWorkflow /
                                          │ QueryWorkflow + Mongo CRUD
                                          ▼
                               ┌──────────────────────┐         ┌─────────────────────┐
                               │  Temporal dev server │◀────────│  Web UI :8233       │
                               │  (SQLite-backed,     │         │  (built into the    │
                               │   embedded UI)       │         │   dev-server image) │
                               └──────────┬───────────┘         └─────────────────────┘
                                          │ task queue: "subflow"
                                          ▼
                               ┌──────────────────────┐
                               │  subflow-worker      │
                               │  - SubscriptionWF    │
                               │  - Activities        │
                               └──────────┬───────────┘
                                          │ activity calls (gRPC) + Mongo writes
                                          ▼
                               ┌──────────────────────┐
                               │  mock-integration    │
                               │  (gRPC :50052)       │
                               │  failure/latency     │
                               │  knobs via env vars  │
                               └──────────────────────┘

┌──────────────────────┐
│  Mongo               │  ◀── subflow database:
│                      │       - plans collection
│                      │       - subscriptions_view collection (read-model projection)
└──────────────────────┘
```

### Three Go binaries

| Binary | Role |
|---|---|
| `subflow-api` | gRPC server. Translates RPCs into Temporal client calls and Mongo CRUD. Contains no business logic. |
| `subflow-worker` | Hosts `SubscriptionWorkflow` and activities. Polls the Temporal task queue. The durable subscription logic lives here. |
| `mock-integration` | gRPC server implementing a tiny `IntegrationService`. Knobs to inject failure rate, latency, and terminal failures. |

### Compose stack (5 services)

1. `mongo` — Mongo 7, subflow database.
2. `temporal` — `temporalio/temporal` running `temporal server start-dev` (SQLite on a volume; Web UI bundled).
3. `subflow-api` — gRPC :50051.
4. `subflow-worker` — no exposed port.
5. `mock-integration` — gRPC :50052.

The compose file works identically under `docker compose` and `podman compose`.

---

## 3. Workflow design

### Ubiquitous language

| Term | Meaning |
|---|---|
| **Subscription** | Aggregate. Lives as a Temporal workflow execution with workflow ID `subscription:<id>`. |
| **Plan** | Aggregate. Defines billing interval, price, and integration endpoint. Stored in Mongo. |
| **BillingPeriod** | Value object: `{Start, End, Sequence}`. Derived from plan. |
| **SubscriptionContext** | Value object: `map[string]string`. Mutable per-period. |
| **Activation** | First lifecycle event (RenewalCount == 0). |
| **Renewal** | Subsequent period-start lifecycle event (RenewalCount > 0). |
| **Deactivation** | Terminal lifecycle event after cancel-and-period-end. |
| **Cancellation** | A signal that flips a flag; deactivation runs at period end. |

### `SubscriptionInput` (workflow input — carried across Continue-As-New)

```go
type SubscriptionInput struct {
    SubscriptionID  string
    UserID          string
    PlanID          string
    PeriodStart     time.Time
    PeriodEnd       time.Time
    Context         map[string]string
    RenewalCount    int   // 0 == activation
    CancelRequested bool
}
```

### Workflow body — reads as a business spec

```go
func SubscriptionWorkflow(ctx workflow.Context, in SubscriptionInput) error {
    if err := StartBillingPeriod(ctx, in); err != nil {
        return err
    }
    cancelled := AwaitPeriodEndOrCancellation(ctx, in)
    if cancelled {
        return DeactivateSubscription(ctx, in)
    }
    return ContinueIntoNextPeriod(ctx, in)
}
```

Each verb is its own exported function in `internal/workflow/lifecycle.go`:

- `StartBillingPeriod` — dispatches to `ActivateSubscription` or `RenewSubscription` based on `IsActivation`.
- `ActivateSubscription` — `ChargePayment` → `PublishSubscriptionEvent("activate")` → `NotifyIntegrationService("activate")` → `UpdateSubscriptionProjection`.
- `RenewSubscription` — same shape with `"renew"` event type.
- `DeactivateSubscription` — `PublishSubscriptionEvent("deactivate")` → `NotifyIntegrationService("deactivate")` → `UpdateSubscriptionProjection`.
- `AwaitPeriodEndOrCancellation` — workflow selector on `(timer fires) | (cancel signal)`. Returns `true` if cancellation occurred (with end-of-period semantics: if cancel arrives early, sleep the remainder).
- `ContinueIntoNextPeriod` — `workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, NextBillingPeriod(in))`.

Pure domain functions live in `internal/domain/subscription/period.go`:

- `NextBillingPeriod(current SubscriptionInput) SubscriptionInput`
- `(SubscriptionInput) IsActivation() bool`

### Continue-As-New strategy: per-renewal

A new workflow run per billing period. Logically identical from outside (workflow ID is unchanged); each run holds bounded history (~20 events). Closed runs are retained per Temporal's retention policy and then garbage-collected.

This pattern keeps a 50-year subscription's per-run history footprint identical to a 1-year one and makes each billing period visible as a discrete row in the Web UI.

### Signals and queries

```go
const (
    SignalCancelSubscription = "subscription.cancel"
    QuerySubscriptionStatus  = "subscription.status"
)

type SubscriptionStatus struct {
    Phase        string // "active" | "cancelling" | "deactivated"
    PeriodStart  time.Time
    PeriodEnd    time.Time
    RenewalCount int
    Context      map[string]string
}
```

`AwaitPeriodEndOrCancellation` watches the signal channel; the query handler returns a snapshot of in-memory state.

### Activities (one file per business capability)

```
internal/activity/
  payment.go       → ChargePayment
  events.go        → PublishSubscriptionEvent
  integration.go   → NotifyIntegrationService
  projection.go    → UpdateSubscriptionProjection
  retry.go         → PaymentRetry / EventPublishingRetry / IntegrationCallRetry
```

Each activity is a top-level exported function. Inputs and outputs are exported structs colocated in the same file.

### Retry policies — named by intent

```go
var (
    PaymentRetry = &temporal.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    time.Minute,
        MaximumAttempts:    5,
        NonRetryableErrorTypes: []string{"InsufficientFundsError", "CardDeclinedError"},
    }
    EventPublishingRetry = &temporal.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 1.5,
        MaximumInterval:    30 * time.Second,
        MaximumAttempts:    0, // unlimited — events should eventually publish
    }
    IntegrationCallRetry = &temporal.RetryPolicy{
        InitialInterval:    time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    time.Minute,
        MaximumAttempts:    0, // unlimited — operator can fix and let it drain
    }
)
```

### Idempotency

Each activity call constructs a `reference` = `{workflowID}:{runID}:{activityID}`. Stable across retries, unique across runs. The mock integration service caches by reference and returns the cached response on duplicates. Real integrations would use the same scheme.

---

## 4. gRPC API

### `subflow.proto`

```protobuf
syntax = "proto3";
package subflow.v1;

import "google/protobuf/timestamp.proto";

service SubflowService {
  rpc CreatePlan(CreatePlanRequest)   returns (Plan);
  rpc GetPlan(GetPlanRequest)         returns (Plan);
  rpc ListPlans(ListPlansRequest)     returns (ListPlansResponse);
  rpc DeletePlan(DeletePlanRequest)   returns (DeletePlanResponse);

  rpc CreateSubscription(CreateSubscriptionRequest) returns (Subscription);
  rpc CancelSubscription(CancelSubscriptionRequest) returns (CancelSubscriptionResponse);
  rpc GetSubscription(GetSubscriptionRequest)       returns (Subscription);
  rpc ListSubscriptions(ListSubscriptionsRequest)   returns (ListSubscriptionsResponse);
}

message Plan {
  string id = 1;
  string code = 2;
  string name = 3;
  string billing_interval = 4;       // Go duration syntax: "30s", "5m", "720h" — chosen for POC simplicity over ISO 8601
  int64 price_cents = 5;
  string integration_endpoint = 6;
}

message Subscription {
  string id = 1;
  string user_id = 2;
  string plan_id = 3;
  string phase = 4;
  google.protobuf.Timestamp period_start = 5;
  google.protobuf.Timestamp period_end = 6;
  int32 renewal_count = 7;
  map<string, string> context = 8;
  bool cancel_requested = 9;
}
```

### `integration.proto`

```protobuf
syntax = "proto3";
package subflow.integration.v1;

service IntegrationService {
  rpc HandleEvent(IntegrationEvent) returns (IntegrationResponse);
}

message IntegrationEvent {
  string reference = 1;     // idempotency token
  string event_type = 2;    // "subscription.activate" | "...renew" | "...deactivate"
  string user_id = 3;
  string plan_code = 4;
  map<string, string> context = 5;
}

message IntegrationResponse {
  map<string, string> updated_context = 1;
}
```

### Handler translation table

| RPC | Translation |
|---|---|
| `CreatePlan / GetPlan / ListPlans / DeletePlan` | Mongo CRUD on `plans` |
| `CreateSubscription` | `client.ExecuteWorkflow(ctx, opts{ID: "subscription:"+subID, TaskQueue: "subflow"}, SubscriptionWorkflow, in)`; insert pending row into `subscriptions_view` |
| `CancelSubscription` | `client.SignalWorkflow(ctx, "subscription:"+subID, "", SignalCancelSubscription, nil)` |
| `GetSubscription` | `client.QueryWorkflow(ctx, "subscription:"+subID, "", QuerySubscriptionStatus)`; if workflow not running, fall back to `subscriptions_view` |
| `ListSubscriptions` | Mongo query on `subscriptions_view` (filters: user_id, phase) |

The handlers contain only translation — no business logic.

---

## 5. Persistence

### Mongo collections (database: `subflow`)

`plans`:

```json
{
  "_id": "<id>",
  "code": "monthly-basic",
  "name": "Monthly Basic",
  "billing_interval": "720h",
  "price_cents": 999,
  "integration_endpoint": "mock-integration:50052",
  "created_at": "ISODate(...)"
}
```

`subscriptions_view`:

```json
{
  "_id": "<id>",
  "user_id": "...",
  "plan_id": "...",
  "phase": "active",
  "period_start": "...",
  "period_end": "...",
  "renewal_count": 3,
  "context": { "card_id": "card_001" },
  "updated_at": "..."
}
```

Indexes:
- `plans.code` unique.
- `subscriptions_view.user_id`.
- `subscriptions_view.phase`.

### Source of truth

Temporal is the source of truth for live subscription state. `subscriptions_view` is a read-model projection updated by `UpdateSubscriptionProjection` after each significant lifecycle event. Listing operations read from the projection because Temporal's Visibility API is not the right tool for ad-hoc listing.

### Temporal persistence

SQLite via the `temporal server start-dev` mode, file mounted on a named volume so workflow state survives container restarts. `task reset` removes the volume to start clean.

---

## 6. Failure scenarios

### Integration service down (transient — retryable)

`NotifyIntegrationService` returns gRPC `Unavailable`. Temporal applies `IntegrationCallRetry` (exponential backoff). Web UI shows the activity scheduled with attempt count climbing. When `mock-integration` is restarted, the next retry succeeds and the workflow proceeds. No code in subflow changes; this is the headline demo of Temporal's retry semantics.

### Payment terminally declined (non-retryable)

`ChargePayment` returns an `ApplicationError` whose type is in `NonRetryableErrorTypes`. Temporal does not retry. The workflow function receives the error, returns it, and the workflow is marked Failed in the Web UI. The projection row remains in its prior phase. A more sophisticated implementation could catch the error and run a soft-fail path; that is an extension, not POC scope.

### Worker process killed mid-period

Temporal sees no heartbeat or activity timeout, reschedules the activity to a (restarted or other) worker, and replay reconstructs workflow state from history. Idempotency tokens prevent double charges. Workflow continues as if uninterrupted.

### Cancel arrives before period end

The signal flips a flag. The workflow remains parked on the timer until `PeriodEnd` (end-of-period semantics — the active billing period runs to completion before deactivation), then runs deactivation activities and completes.

---

## 7. Repository layout

```
subflow/
├── README.md
├── LICENSE                   # MIT
├── go.mod
├── Taskfile.yml
├── compose.yml               # podman / docker compose compatible
├── .env.example
├── api/v1/
│   ├── subflow.proto
│   ├── integration.proto
│   └── *.pb.go               # generated
├── buf.yaml / buf.gen.yaml
├── cmd/
│   ├── api/main.go
│   ├── worker/main.go
│   └── mock-integration/main.go
├── internal/
│   ├── domain/
│   │   ├── subscription/
│   │   │   ├── input.go      # SubscriptionInput
│   │   │   ├── period.go     # NextBillingPeriod, BillingPeriod
│   │   │   └── context.go
│   │   └── plan/plan.go
│   ├── workflow/
│   │   ├── subscription.go   # SubscriptionWorkflow entry
│   │   ├── lifecycle.go      # ActivateSubscription, RenewSubscription, ...
│   │   ├── signals.go
│   │   ├── state.go          # in-run state struct + query handler
│   │   └── *_test.go
│   ├── activity/
│   │   ├── payment.go events.go integration.go projection.go
│   │   ├── retry.go
│   │   └── *_test.go
│   ├── server/
│   │   ├── plans.go
│   │   └── subscriptions.go
│   ├── store/
│   │   ├── mongo.go
│   │   ├── plans.go
│   │   └── projection.go
│   ├── eventbus/stub.go      # stdout sink behind a small interface
│   ├── integration/client.go
│   └── config/config.go
└── docs/
    ├── architecture.md
    ├── temporal-cheatsheet.md
    └── superpowers/specs/2026-05-09-subflow-design.md
```

### Stack choices

| Concern | Choice | Why |
|---|---|---|
| Go version | 1.23+ | Modern; matches author's primary stack |
| gRPC | `google.golang.org/grpc` | Standard |
| Proto codegen | `buf` | Cleaner than raw protoc |
| Mongo driver | `go.mongodb.org/mongo-driver/v2` | Official, current major |
| Temporal SDK | `go.temporal.io/sdk` | Required |
| Test framework | stdlib + Temporal `testsuite` | OSS minimal — `testify` can be added if dev tests grow |
| Logging | `log/slog` (stdlib) | Modern Go default |
| Config | env vars + tiny loader | Minimal |

### Three Dockerfiles

`cmd/api/Dockerfile`, `cmd/worker/Dockerfile`, `cmd/mock-integration/Dockerfile`. Multi-stage Go builds, distroless final stages.

---

## 8. Testing

| Layer | Approach |
|---|---|
| Pure domain (`NextBillingPeriod`, `IsActivation`) | Plain Go table tests |
| Activities | Go tests with stub clients (interface mocks for Mongo / integration gRPC) |
| Workflow | `temporal.io/sdk/testsuite` with mocked activities and time-skipping |
| gRPC handlers | Direct calls to handlers with Temporal client interface mocked |
| Manual / e2e | `task up` + grpcurl flows documented in README |

A handful of representative tests, not exhaustive. Goal is to anchor core logic (workflow lifecycle, period math). Manual testing is the primary validation method.

---

## 9. Developer workflow

```bash
task up                          # podman compose up -d
task db-init                     # apply Mongo indexes (one-shot)
task logs                        # tail all services
open http://localhost:8233       # Temporal Web UI

task seed-plan                   # CreatePlan via grpcurl
task create-subscription USER=alice PLAN=monthly-basic
task cancel-subscription ID=<sub-id>

# Failure injection
podman compose stop mock-integration
podman compose start mock-integration

task reset                       # nuke Mongo + Temporal volumes
```

### README outline

1. What this is
2. Quickstart (clone → `task up` → three grpcurl commands)
3. What you'll see (Web UI + the four most interesting things to look for)
4. Architecture diagram
5. Concept cheatsheet (subscription concept ↔ Temporal primitive)
6. Failure-injection playground
7. Roadmap (workflow versioning, plan changes, schedule-based admin operations)

---

## 10. Out of scope (explicit)

- Plan upgrades/downgrades mid-period.
- Pause/resume.
- Refunds.
- Authentication, authorization, multi-tenancy.
- Real payment integration.
- Real event bus integration (Kafka, etc.) — interface exists; implementation is stdout.
- Workflow versioning helpers (`workflow.GetVersion`).
- Production-grade Temporal deployment (Cassandra, multi-region).

These are documented as a "Roadmap" section of the README so readers know what a real deployment would add.

---

## 11. Open questions

None blocking. To be revisited if the POC graduates beyond a learning playground.
