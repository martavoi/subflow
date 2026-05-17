# Workflow consolidation & naming refactor

Status: draft
Date: 2026-05-17

Refactor the workflow layer for naming consistency, package consolidation, and method-count reduction. No behavior changes — pure rename + restructure.

## Why

Four problems, all visible after the recent hook collapse (ADR 0003):

1. `workflow.Status` and the `subscription.status` query violate the project's own glossary. `CONTEXT.md` lists "Status" under `_Avoid_` for `Phase`. The thing being returned is a read-model view of the subscription, not a status.
2. `plan.Snapshot` is a parallel type to `plan.Plan` whose only difference is omitting four fields (`ID`, `Name`, `PerUserLimit`, `CreatedAt`). The extra type costs more mental overhead than the saved bytes are worth.
3. `internal/activity` holds three handler structs (~150 lines combined) plus retry policies and error constants. The canonical Temporal Go layout (samples-go, the eCommerce tutorial) keeps activities and workflows in one package; the split here is ceremony.
4. `fireLifecycle` and `firePayment` in `workflow/hook.go` are ~90% identical. They differ only in which oneof of `DispatchHook` is populated and in the `EventID` format.

## Scope

In scope:
- Rename `workflow.Status` → `workflow.View` (type, query name, handler method).
- Drop `plan.Snapshot` and `plan.SnapshotOf`. Workflow input carries full `plan.Plan`.
- Fold `internal/activity` into `internal/workflow`. Delete `internal/activity`.
- Introduce `hook.Event` as the integration-event contract with a sealed `hook.Payload` interface (`LifecyclePayload`, `PaymentPayload`). Replace `fireLifecycle`/`firePayment` with `emitLifecycle`/`emitPayment` + an internal `dispatch` helper.
- Split `workflow/phases.go` into one file per phase method.
- Four ADRs documenting the decisions (0004 View, 0005 drop Snapshot, 0006 fold activity package, 0007 integration-event contract).
- CONTEXT.md addition noting `View` as the read-model and clarifying that Hook events are **integration events**, not domain events in the DDD sense.

Out of scope:
- No proto/gRPC changes.
- No persistence/store changes.
- No retry policy changes.
- No `hook.Type` rename. `hook.Type` already reads cleanly inside `package hook`.
- No `Phase` enum changes. `CONTEXT.md` is firm on those.

## Decisions

### D1. `Status` → `View`

- Type: `workflow.View` (was `workflow.Status`)
- Query name: `subscription.view` (was `subscription.status`)
- Constant: `QuerySubscriptionView` (was `QuerySubscriptionStatus`)
- Handler method: `(*Subscription).View()` (was `AsStatus`)

CONTEXT.md gets a single new sentence under the Subscription glossary entry, e.g. "The **View** is the read-model returned by the `subscription.view` query — a snapshot of the entity for API rendering, not a lifecycle state. Phase is the lifecycle state."

### D2. Drop `plan.Snapshot`

- Delete `Snapshot` struct and `SnapshotOf` function from `internal/domain/plan/plan.go`.
- `SubscriptionInput.Plan` becomes `plan.Plan`.
- Subscription entity field `Plan` becomes `plan.Plan`.
- Test helpers in `subscription_test.go` and `input_test.go` updated to construct `plan.Plan` directly. The mods callback signature changes from `func(*plan.Snapshot)` to `func(*plan.Plan)`.

The extra fields (`ID`, `Name`, `PerUserLimit`, `CreatedAt`) ride along on the workflow input. They're cheap and unused — explicitly so. No code should read them inside the workflow.

### D3. Fold `internal/activity` into `internal/workflow`

`internal/activity/` is deleted. The five files move into `internal/workflow/` with these names:

| from | to |
| --- | --- |
| `activity/payment.go` | `workflow/activity_payment.go` |
| `activity/billing.go` | `workflow/activity_billing.go` |
| `activity/hooks.go` | `workflow/activity_hook.go` |
| `activity/retry.go` | `workflow/retry.go` |
| `activity/errors.go` | `workflow/errors.go` |

Package declaration in every file: `package workflow`.

The `activity_` prefix matches the existing `phase_` convention and signals "this file holds a Temporal activity handler — must not be called from inside a workflow function". File-naming discipline is the only convention; Go has no language-level guard.

The worker (`cmd/worker/main.go`) drops its `activity` import. All registration calls reference `workflow` instead:

```go
paymentActs := &workflow.PaymentGateway{...}
billingActs := &workflow.BillingStore{Events: billingStore}
hookActs    := &workflow.HookDispatcher{Client: intClient}
```

Activity registration names: `"ChargePayment"`, `"RecordBillingEvent"`, `"DispatchHook"`. These are verb-noun and read well; no renaming proposed.

### D4. Integration-event contract: `hook.Event` with sealed payload

The previous `fireLifecycle` / `firePayment` split mixed two concerns: (a) the per-payload-category data shape, and (b) the dispatch boilerplate. Split them. Introduce a canonical `hook.Event` type. Use a sealed-interface `hook.Payload` for category variance — no nullable parameters, compile-time enforcement that every event carries a typed payload.

**New file `internal/hook/event.go`:**

```go
package hook

import "time"

// Event is the canonical integration-event payload published to integrators.
// It is an integration event (DDD-speak) — published outward, never used to
// drive internal subscription state. See ADR 0002.
type Event struct {
    Type           Type
    SubscriptionID string
    UserID         string
    PlanCode       string
    RenewalCount   int
    OccurredAt     time.Time
    Context        map[string]string
    Payload        Payload   // sealed; required.
}

// Payload is the category-specific variant carried on an Event. The interface
// is sealed via the private isPayload marker — only payload types declared
// in this package can satisfy it. New categories add a struct + isPayload.
type Payload interface{ isPayload() }

type LifecyclePayload struct {
    Phase       string
    PeriodStart time.Time
    PeriodEnd   time.Time
}
func (LifecyclePayload) isPayload() {}

type PaymentPayload struct {
    DunningAttempt int
    AmountCents    int64
    Currency       string
    TransactionID  string
    FailureReason  string
}
func (PaymentPayload) isPayload() {}
```

The previous `workflow.LifecycleData` and `workflow.PaymentData` types are deleted; their fields move to `hook.LifecyclePayload` / `hook.PaymentPayload`.

**Activity input `workflow.DispatchHook` wraps the canonical event with delivery metadata:**

```go
type DispatchHook struct {
    Event    hook.Event
    Endpoint string
    EventID  string   // idempotency key
}
```

The `HookDispatcher.Dispatch` activity does a type switch on `in.Event.Payload` to populate the proto `Event` oneof:

```go
switch p := in.Event.Payload.(type) {
case hook.LifecyclePayload:
    ev.Data = &subflowv1.Event_Lifecycle{Lifecycle: &subflowv1.LifecycleData{...}}
case hook.PaymentPayload:
    ev.Data = &subflowv1.Event_Payment{Payment: &subflowv1.PaymentData{...}}
}
```

**Workflow surface: two typed emit methods + one internal dispatch helper.** Lifecycle data is entirely subscription-derived (no extra args); payment data is call-site supplied (required).

```go
// workflow/hook.go

// emitLifecycle fires a lifecycle hook. All payload data comes from current
// subscription state.
func (s *Subscription) emitLifecycle(ctx workflow.Context, t hook.Type) error {
    return s.dispatch(ctx, hook.Event{
        Type: t,
        Payload: hook.LifecyclePayload{
            Phase:       string(s.Phase),
            PeriodStart: s.Period.Start,
            PeriodEnd:   s.Period.End,
        },
    })
}

// emitPayment fires a payment hook. Caller supplies the full payload — no
// magic fill-in. Amount/currency come from the plan at the call site.
func (s *Subscription) emitPayment(ctx workflow.Context, t hook.Type, p hook.PaymentPayload) error {
    return s.dispatch(ctx, hook.Event{Type: t, Payload: p})
}

// dispatch is the internal common-path: plan gating, common-field fill-in,
// idempotency key, activity invocation.
func (s *Subscription) dispatch(ctx workflow.Context, e hook.Event) error {
    if s.Plan.IntegrationEndpoint == "" || !isEnabled(e.Type, s.Plan.EnabledHooks) {
        return nil
    }
    e.SubscriptionID = s.SubscriptionID
    e.UserID         = s.UserID
    e.PlanCode       = s.PlanCode
    e.RenewalCount   = s.RenewalCount
    e.OccurredAt     = workflow.Now(ctx)
    e.Context        = map[string]string(s.Context)

    in := DispatchHook{
        Event:    e,
        Endpoint: s.Plan.IntegrationEndpoint,
        EventID:  s.hookID(e),
    }
    opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        RetryPolicy:         HookRetry,
    })
    return workflow.ExecuteActivity(opts, "DispatchHook", in).Get(ctx, nil)
}

// hookID derives the idempotency key based on payload type. Payment payloads
// include DunningAttempt so retries on the same hook type across dunning
// attempts get distinct event IDs.
func (s *Subscription) hookID(e hook.Event) string {
    if p, ok := e.Payload.(hook.PaymentPayload); ok {
        return s.idempotencyKey(fmt.Sprintf("hook:%s:%d", e.Type, p.DunningAttempt))
    }
    return s.idempotencyKey("hook:" + string(e.Type))
}
```

**Call-site changes:**
- `s.fireLifecycle(ctx, hook.Activated)` → `s.emitLifecycle(ctx, hook.Activated)`
- `s.firePayment(ctx, hook.PaymentOK, dunningAttempt, txnID, "")` →
  ```go
  s.emitPayment(ctx, hook.PaymentOK, hook.PaymentPayload{
      DunningAttempt: dunningAttempt,
      AmountCents:    s.Plan.PriceCents,
      Currency:       s.Plan.Currency,
      TransactionID:  txnID,
  })
  ```

**Extension point.** Adding a new payload category (e.g. plan change, proration):
1. Add struct + `isPayload()` in `internal/hook/event.go`.
2. Add proto oneof variant in `api/v1/hooks.proto` and regenerate.
3. Add type-switch arm in `HookDispatcher.Dispatch`.
4. Add a new `emitXxx` method on `*Subscription` that constructs an event with the new payload.

Existing payload types and emit methods don't change.

### D5. Phase-per-file split

`workflow/phases.go` is deleted. Eight methods spread across seven files:

| file | methods |
| --- | --- |
| `phase_trial.go` | `Trial` + `trialOutcome` enum |
| `phase_activation.go` | `AwaitActivation`, `Activate`, `ActivationResult` |
| `phase_renewal.go` | `Renew` |
| `phase_dunning.go` | `Dun`, `ErrDunningExhausted` |
| `phase_await_end.go` | `AwaitEnd`, `cancelAndSleep` |
| `phase_deactivation.go` | `Deactivate` |
| `phase_next_period.go` | `NextPeriod`, `toInput` |

Helpers stay with the methods they serve. No new exported surface.

## Final file layout (`internal/workflow/`)

```
subscription.go          entity struct, NewSubscription, SubscriptionWorkflow, Run, transitionTo, idempotencyKey
input.go                 SubscriptionInput, Context, NextBillingPeriod
signals.go               signal/query/update name constants
handlers.go              query + signal handlers on *Subscription
view.go                  View struct (was status.go)
charge.go                (*Subscription).Charge orchestration + chargePurpose
hook.go                  (*Subscription).emitLifecycle, emitPayment, dispatch, hookID, isEnabled

phase_trial.go
phase_activation.go
phase_renewal.go
phase_dunning.go
phase_await_end.go
phase_deactivation.go
phase_next_period.go

activity_payment.go      PaymentGateway, ChargePayment, ChargeResult
activity_billing.go      BillingStore, RecordBillingEvent
activity_hook.go         HookDispatcher, DispatchHook (wraps hook.Event)

retry.go                 ChargePaymentRetry, BillingEventRetry, HookRetry
errors.go                error type constants

subscription_test.go
input_test.go
```

## File layout (`internal/hook/`)

```
hook.go                  Type enum, All, Parse (unchanged)
event.go                 Event, Payload (sealed), LifecyclePayload, PaymentPayload  (new)
hook_test.go             unchanged
```

## ADRs to write

- `docs/adr/0004-rename-status-to-view.md` — why View, what was wrong with Status, language-guide alignment.
- `docs/adr/0005-drop-plan-snapshot.md` — why the parallel type is overkill for a learning project; the unused fields are explicit.
- `docs/adr/0006-fold-activity-into-workflow-package.md` — canonical Temporal Go layout reference; the `activity_` prefix as the only discipline; risk of accidental coupling and mitigation.
- `docs/adr/0007-integration-event-contract.md` — `hook.Event` with sealed `Payload` variant; integration-event (not domain-event) framing in DDD terms; extension is a new payload struct + emit method + proto oneof; what was rejected (Kafka, per-hook-typed structs, one method with nullable args).

## Verification

After the refactor:
- `go build ./...` passes.
- `go test ./...` passes (existing tests adapted for renamed types).
- Worker starts; activities (`ChargePayment`, `RecordBillingEvent`, `DispatchHook`) and the workflow (`SubscriptionWorkflow`) register without error.
- Manual: query a running subscription via the API; `subscription.view` returns the same shape `subscription.status` did (modulo the type name).
- `internal/activity/` directory does not exist.
- `grep -r "plan.Snapshot" internal/` returns nothing.
- `grep -r "workflow.Status\b" internal/` returns nothing.
- `grep -r "fireLifecycle\|firePayment" internal/` returns nothing.
- `grep -rn "LifecycleData\|PaymentData" internal/workflow/` returns nothing (they live in `internal/hook/` now as `*Payload`).
