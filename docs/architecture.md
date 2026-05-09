# Architecture

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
                               │   dev-server image) │         │   dev-server image) │
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
                               └──────────────────────┘

┌──────────────────────┐
│  Mongo               │
│  - plans             │
│  - subscriptions_view│
└──────────────────────┘
```

## Components

| Component | Role |
|---|---|
| `subflow-api` | gRPC server on :50051. Translates RPCs into Temporal client calls and Mongo CRUD. No business logic. |
| `subflow-worker` | Temporal worker. Hosts `SubscriptionWorkflow` and the four activities. Polls the `subflow` task queue. |
| `mock-integration` | gRPC server on :50052 implementing `IntegrationService`. Configurable failure/latency knobs. |
| `temporal` | Single-binary dev server. SQLite persistence. Bundled Web UI on :8233. |
| `mongo` | Mongo 7. Holds plans collection (source of truth) and subscriptions_view collection (read-model). |

## Why this shape?

- **Workflow as state machine**: each subscription is *one workflow execution* (per period, via Continue-As-New). The workflow carries its own state (period, context, cancel flag) — no polling scheduler scanning a database.
- **Activities for I/O**: payment, event publishing, integration callouts, projection writes — each with its own retry policy named for the failure mode it handles.
- **Read-model projection**: Temporal is the source of truth for live state; Mongo's `subscriptions_view` exists only because Temporal Visibility isn't the right tool for ad-hoc listing.
- **Mock integration**: a tiny gRPC service with failure-injection knobs makes Temporal's retry semantics tangible — stop the container, watch backoffs in the Web UI.
