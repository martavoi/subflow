# Architecture

```
┌─────────────────┐    gRPC    ┌──────────────────────┐
│  client (CLI/   │──────────▶│  subflow-api         │
│  grpcurl)       │            │  (gRPC :50051)       │
└─────────────────┘            └──────────┬───────────┘
                                          │ StartWorkflow / Signal /
                                          │ Query / Update /
                                          │ ListWorkflow / CountWorkflow
                                          │ + Mongo CRUD (plans, billing_events)
                                          ▼
                               ┌──────────────────────┐         ┌─────────────────────┐
                               │  Temporal dev server │◀────────│  Web UI :8233       │
                               │  (SQLite-backed,     │         │  (bundled)          │
                               │   custom SAs)        │         └─────────────────────┘
                               └──────────┬───────────┘
                                          │ task queue: "subflow"
                                          ▼
                               ┌──────────────────────┐
                               │  subflow-worker      │
                               │  - SubscriptionWF    │
                               │  - 12 activities:    │
                               │    ChargePayment     │
                               │    RecordBillingEv.  │
                               │    OnTrialStarted    │
                               │    OnTrialWillEnd    │
                               │    OnActivated       │
                               │    OnRenewed         │
                               │    OnPastDue         │
                               │    OnRecovered       │
                               │    OnCanceled        │
                               │    OnDeactivated     │
                               │    OnPaymentSucc.    │
                               │    OnPaymentFailed   │
                               └──────────┬───────────┘
                                          │ gRPC calls + Mongo writes
                                          ▼
                               ┌──────────────────────┐
                               │  mock-integration    │
                               │  (gRPC :50052,       │
                               │   SubscriptionHooks) │
                               └──────────────────────┘

┌──────────────────────┐
│  Mongo               │
│  - plans             │  ← static config
│  - billing_events    │  ← append-only history (idempotent on reference)
└──────────────────────┘
```

## Component responsibilities

| Component | Role |
|---|---|
| `subflow-api` | gRPC on :50051. Translates RPCs to Temporal operations (Start/Signal/Update/Query/List/Count) + Mongo CRUD. Registers custom search attributes at startup. No business logic. |
| `subflow-worker` | Temporal worker. Hosts `SubscriptionWorkflow` and 12 activities. Polls the `subflow` task queue. |
| `mock-integration` | gRPC on :50052 implementing `SubscriptionHooks` (10 methods). Failure/latency knobs for retry demos. |
| `temporal` | Single-binary dev server. SQLite persistence (workflow histories + visibility index). Bundled Web UI on :8233. Custom search attributes registered idempotently at startup. |
| `mongo` | Mongo 7. Holds `plans` (source of truth for plan config) and `billing_events` (append-only charge history). |

## Why this shape?

- **Workflow as entity.** Each subscription is one workflow execution (per period, via Continue-As-New). State + behavior co-located on the `Subscription` struct. No projection mirror in a side DB.
- **Visibility for listing.** Custom search attributes drive `ListWorkflow` and `CountWorkflow`. Per-user limit enforcement, "expiring soon" queries, "all past_due" — all without an extra index.
- **Append-only billing.** History grows unboundedly so it lives outside the workflow (would blow the 2MB payload limit otherwise). One activity write per charge attempt; Mongo's unique index on `reference` makes the write idempotent under Temporal retries.
- **Pure-notification hooks.** Modeled on Stripe / Recurly / Chargebee / Paddle / Apple / Google. Workflow doesn't await integrator approval. Integrators that need to mutate state call back via `UpdateSubscriptionContext`.
- **Sync activation, async renewals.** `UpdateWithStartWorkflow` makes `CreateSubscription` block on the activation outcome for non-trial plans. Renewals run on the workflow's durable timer with no synchronous caller.
