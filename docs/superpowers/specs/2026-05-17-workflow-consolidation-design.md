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
- Collapse `fireLifecycle` + `firePayment` into one `(*Subscription).emit(ctx, hook.Type, *paymentArgs)`.
- Split `workflow/phases.go` into one file per phase method.
- Three ADRs documenting the decisions.
- CONTEXT.md addition noting `View` as the read-model.

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

### D4. One `emit` replacing `fireLifecycle` + `firePayment`

```go
type paymentArgs struct {
    DunningAttempt int
    TransactionID  string
    FailureReason  string
}

func (s *Subscription) emit(ctx workflow.Context, h hook.Type, p *paymentArgs) error {
    if s.Plan.IntegrationEndpoint == "" || !isEnabled(h, s.Plan.EnabledHooks) {
        return nil
    }
    in := DispatchHook{
        Endpoint:       s.Plan.IntegrationEndpoint,
        Type:           h,
        SubscriptionID: s.SubscriptionID,
        UserID:         s.UserID,
        PlanCode:       s.PlanCode,
        RenewalCount:   s.RenewalCount,
        EventTime:      workflow.Now(ctx),
        Context:        map[string]string(s.Context),
    }
    if p == nil {
        in.EventID = s.idempotencyKey("hook:" + string(h))
        in.Lifecycle = &LifecycleData{
            Phase:       string(s.Phase),
            PeriodStart: s.Period.Start,
            PeriodEnd:   s.Period.End,
        }
    } else {
        in.EventID = s.idempotencyKey(fmt.Sprintf("hook:%s:%d", h, p.DunningAttempt))
        in.Payment = &PaymentData{
            DunningAttempt: p.DunningAttempt,
            AmountCents:    s.Plan.PriceCents,
            Currency:       s.Plan.Currency,
            TransactionID:  p.TransactionID,
            FailureReason:  p.FailureReason,
        }
    }
    opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        RetryPolicy:         HookRetry,
    })
    return workflow.ExecuteActivity(opts, "DispatchHook", in).Get(ctx, nil)
}
```

Call-site changes:
- `s.fireLifecycle(ctx, hook.Activated)` → `s.emit(ctx, hook.Activated, nil)`
- `s.firePayment(ctx, hook.PaymentOK, dunningAttempt, txnID, "")` → `s.emit(ctx, hook.PaymentOK, &paymentArgs{DunningAttempt: dunningAttempt, TransactionID: txnID})`

Behavior is identical: same payload, same retry policy, same idempotency-key format for both variants.

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
hook.go                  (*Subscription).emit + paymentArgs + isEnabled

phase_trial.go
phase_activation.go
phase_renewal.go
phase_dunning.go
phase_await_end.go
phase_deactivation.go
phase_next_period.go

activity_payment.go      PaymentGateway, ChargePayment, ChargeResult
activity_billing.go      BillingStore, RecordBillingEvent
activity_hook.go         HookDispatcher, DispatchHook, LifecycleData, PaymentData

retry.go                 ChargePaymentRetry, BillingEventRetry, HookRetry
errors.go                error type constants

subscription_test.go
input_test.go
```

## ADRs to write

- `docs/adr/0004-rename-status-to-view.md` — why View, what was wrong with Status, language-guide alignment.
- `docs/adr/0005-drop-plan-snapshot.md` — why the parallel type is overkill for a learning project; the unused fields are explicit.
- `docs/adr/0006-fold-activity-into-workflow-package.md` — canonical Temporal Go layout reference; the `activity_` prefix as the only discipline; risk of accidental coupling and mitigation.

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
