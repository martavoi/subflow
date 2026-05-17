# subflow ↔ Temporal cheat sheet

## Concept mapping

| Subscription concept | Temporal primitive | Code location |
|---|---|---|
| Subscription instance | Workflow execution (ID `subscription:<id>`) | `internal/workflow/subscription.go` |
| Sync activation | Update `subscription.activate` via `client.UpdateWithStartWorkflow` | `internal/workflow/activation.go` + `internal/server/subscriptions.go` |
| Renewal cadence | `workflow.NewTimer` durable timer | `internal/workflow/cancellation.go` |
| End of period | `workflow.NewContinueAsNewError` | `internal/workflow/continuation.go` |
| Cancel | Signal `subscription.cancel` | `SignalCancelSubscription` constant |
| Update context | Signal `subscription.update_context` | `SignalUpdateContext` constant |
| View read | Query `subscription.view` | `internal/workflow/handlers.go` View |
| List/filter subscriptions | `client.ListWorkflow` + custom search attributes | `internal/server/subscriptions.go` |
| Per-user limit | `client.CountWorkflow` | `internal/server/subscriptions.go` countActiveForUser |
| Charge | Activity `ChargePayment` with retry policy | `internal/workflow/activity_payment.go` |
| Billing history | Activity `RecordBillingEvent` writing to `billing_events` | `internal/workflow/activity_billing.go` + `internal/billing/mongo_store.go` |
| Lifecycle hooks | 1 activity (`DispatchHook`) dispatching to a single `Dispatch` rpc on the integration | `internal/workflow/activity_hook.go` |
| Renewal-upcoming notice | `workflow.AwaitWithTimeout` phase-1 timer at `Period.End - RenewalUpcomingBefore` | `internal/workflow/subscription.go` `Run` |
| Dunning loop | `workflow.Sleep` + `workflow.UpsertSearchAttributes` | `internal/workflow/dunning.go` |
| Idempotency token | `<workflowID>:<runID>:<purpose>` | `(*Subscription).idempotencyKey` |

## Hook wire names (11 hooks)

| Wire name | Fires when |
|---|---|
| `subscription.trial_started` | Workflow enters trialing phase |
| `subscription.trial_will_end` | `TrialEndNoticeBefore` before trial ends |
| `subscription.renewal_upcoming` | `RenewalUpcomingBefore` before a paid renewal Charge |
| `subscription.activated` | First payment succeeded |
| `subscription.renewed` | Nth payment succeeded (N ≥ 1) |
| `subscription.past_due` | Renewal charge failed, entering dunning |
| `subscription.recovered` | Dunning retry succeeded |
| `subscription.canceled` | Cancel signal received |
| `subscription.deactivated` | Workflow ending (terminal) |
| `payment.succeeded` | Each successful charge |
| `payment.failed` | Each failed charge attempt |

## Custom search attributes

| Name | Type | Set at start? | Upserted by workflow? |
|---|---|---|---|
| `SubflowUserId` | Keyword | Yes (`StartWorkflowOptions`) | No (immutable; carried by CAN) |
| `SubflowPlanCode` | Keyword | Yes | No (immutable; carried by CAN) |
| `SubflowPhase` | Keyword | Yes (initial) | Yes (`transitionTo` on every state change) |
| `SubflowPeriodEnd` | Datetime | Yes | Yes (`ContinueIntoNextPeriod` updates before CAN) |
| `SubflowTrialEnd` | Datetime | Conditionally (when `TrialDuration > 0`) | Set in `Trial()` |

Search attribute values are **carried across Continue-As-New automatically by the Temporal server** — only call `UpsertSearchAttributes` when a value actually changes.

## Visibility queries you can run

```sql
-- All active subscriptions for a user
SubflowUserId='alice' AND SubflowPhase='active'

-- Subscriptions expiring in the next 7 days
SubflowPhase='active' AND SubflowPeriodEnd < '2026-05-19T00:00:00Z'

-- Premium-plan past_due (dunning candidates)
SubflowPlanCode='premium' AND SubflowPhase='past_due'

-- All terminal subscriptions (closed workflows still indexed for the retention window)
SubflowPhase='deactivated'

-- Trial subscriptions ending today
SubflowPhase='trialing' AND SubflowTrialEnd < '2026-05-13T00:00:00Z'
```

## Things to try in the Web UI (http://localhost:8233)

1. **Open a subscription workflow** (search by `WorkflowId STARTS_WITH 'subscription:'`). Watch each CAN run appear as a discrete row in the execution chain.
2. **Check pending activities** when `mock-integration` is down (`task break-integration`) — hooks accumulate retries with exponential backoff.
3. **Send a query from the UI**: Workflow → Query → `subscription.view`. Returns the live snapshot.
4. **Send a signal from the UI**: Workflow → Send Signal → `subscription.cancel`. Watch the workflow honor end-of-period.
5. **Run `temporal operator search-attribute list`** to confirm `SubflowUserId/PlanCode/Phase/PeriodEnd/TrialEnd` are registered.

## Why Continue-As-New per renewal?

Each billing period is its own discrete run with bounded history (~20 events). The workflow ID never changes; signals and queries continue to address the latest run. Keeps history footprint identical for a 1-year and a 50-year subscription, and makes each period visible as one row in the UI. CAN auto-carries search attributes, so the next run inherits all filtering visibility without re-upserting (except `SubflowPeriodEnd`, which changes per period).
