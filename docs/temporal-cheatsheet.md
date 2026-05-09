# subflow ↔ Temporal cheat sheet

| Subscription concept | Temporal primitive | Where in code |
|---|---|---|
| Subscription instance | Workflow execution (ID `subscription:<id>`) | `internal/workflow/subscription.go` |
| Renewal cadence | `workflow.NewTimer` durable timer | `AwaitPeriodEndOrCancellation` in `internal/workflow/lifecycle.go` |
| End of subscription period | Continue-As-New | `ContinueIntoNextPeriod` |
| Cancel request | Signal `subscription.cancel` | `SignalCancelSubscription` constant |
| Status read | Query `subscription.status` | `QuerySubscriptionStatus` constant + `SubscriptionState.AsStatus` |
| Charge / publish / integration call | Activities with named retry policy | `internal/activity/*.go` |
| Mutable subscription context | Workflow state (carried in `SubscriptionInput.Context`) | `internal/domain/subscription/context.go` |
| Read-model for listing | Mongo `subscriptions_view` updated by `UpdateSubscriptionProjection` activity | `internal/store/projection.go` |
| Idempotency token | `<workflowID>:<runID>:<suffix>` | `activityRef` in `internal/workflow/lifecycle.go` |

## Things to play with in the Web UI (http://localhost:8233)

1. **Find a running subscription workflow.** It's `subscription:<uuid>`. Open it.
2. **Look at "Pending Activities"** — when mock-integration is down, this fills with retries.
3. **Click "Continue As New"** events in the history — each renewal is one of these.
4. **Send a signal from the UI**: Workflow → Send Signal → `subscription.cancel`. Watch the timer fire at period end and deactivation activities run.
5. **Run a query from the UI**: Workflow → Query → `subscription.status`. Returns the live in-memory state.

## Why Continue-As-New per renewal (not every N renewals)?

Each billing period is its own discrete run with bounded history (~20 events). The workflow ID never changes; signals and queries continue to address the latest run automatically. This keeps history footprint identical for a 1-year and a 50-year subscription, and makes each period visible as one row in the UI — the renewal history *is* the workflow history.
