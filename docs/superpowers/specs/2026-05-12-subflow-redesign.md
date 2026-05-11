# subflow — Temporal-native redesign

**Date:** 2026-05-12
**Status:** Draft, pending user approval
**Supersedes:** `2026-05-09-subflow-design.md` (initial design)

This redesign replaces the Mongo `subscriptions_view` projection with Temporal-native visibility, restructures the subscription as a struct-based entity workflow, expands the lifecycle state machine to industry-standard scope (5 phases, 10 hooks), introduces a hook integration contract modeled on real subscription platforms, and adds an append-only `billing_events` store.

---

## 1. Why redesign

The original design used Mongo for two distinct purposes: holding plan configuration (correct) and mirroring subscription state in a `subscriptions_view` projection (anti-pattern). Temporal's own guidance flags duplicate state management as something to avoid: "running separate state machines alongside workflows creates conflicting logic and split-brain scenarios."

Three concrete problems with the original:

1. **Two sources of truth for the same data.** Workflow holds live state; Mongo projection holds a stale copy. Listing requires the projection; reading-after-write may show stale data. Updates must be kept in sync.
2. **Reinvents what Temporal already provides.** Visibility's `ListWorkflow` with custom search attributes answers the same questions (filter by user, plan, phase, expiry) without a projection.
3. **Hook contract diverged from industry conventions.** No major billing platform (Stripe, Recurly, Chargebee, Paddle, Lemon Squeezy, Apple, Google) implements flow-control hooks. All are notification-only. The original "PrePayment can reject" was non-idiomatic.

The redesign also expands scope to a more realistic lifecycle: trial periods, dunning/grace, recovery — the states every real subscription platform models.

---

## 2. Goals

Functional:
- Subscriptions go through a 5-state lifecycle: `trialing` → `active` → `past_due` → (canceled →) `deactivated`, with `past_due → active` recovery.
- Plans configure: cadence, price, currency, trial duration, dunning policy, per-user subscription limit, hook opt-in list.
- 10 lifecycle hooks (8 subscription-level + 2 payment-level) — all optional per plan, fire-and-forget, no flow control.
- Billing history persisted as append-only Mongo events queryable via `ListBillingEvents`.
- Listing/filtering subscriptions runs via Temporal Visibility, no projection.

Non-functional:
- Subscription state lives only in the workflow. No projection mirror.
- Workflow state stays O(1) — no unbounded lists. Historical data goes to dedicated stores.
- Workflow code follows the struct-based entity pattern. Each domain verb is a method on `Subscription`.
- Activities are the only place I/O happens. Workflow code is deterministic.

---

## 3. Out of scope (explicit, deferred to roadmap)

- Plan upgrades / downgrades (mid-period plan changes).
- Pause / resume.
- Refunds (the `payment.refunded` hook is reserved but not implemented).
- Coupons / promotional pricing.
- Proration on plan change.
- Multi-currency conversion at billing time.
- Tax computation.
- Real payment gateway integration (`ChargePayment` stays mocked).
- Workflow versioning helpers (`workflow.GetVersion`).
- Kafka as billing event transport (interface exists; only Mongo implementation in POC).
- Multi-tenant namespacing in Temporal.

The roadmap section of the README documents these for readers.

---

## 4. System architecture

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
                               │  (SQLite-backed,     │         │  (built into the    │
                               │   embedded UI,       │         │   dev-server image) │
                               │   custom SAs)        │         └─────────────────────┘
                               └──────────┬───────────┘
                                          │ task queue: "subflow"
                                          ▼
                               ┌──────────────────────┐
                               │  subflow-worker      │
                               │  - SubscriptionWF    │
                               │  - Activities:       │
                               │    ChargePayment     │
                               │    RecordBillingEv   │
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
                               │   implements         │
                               │   SubscriptionHooks) │
                               └──────────────────────┘

┌──────────────────────┐
│  Mongo               │
│  - plans             │  ← static config; PlanRepository (unchanged in shape)
│  - billing_events    │  ← append-only history; BillingEventRepository (new)
│                      │
│  - subscriptions_view│  ← DELETED. Temporal Visibility replaces it.
└──────────────────────┘
```

Three binaries, five compose services (unchanged from initial design). Behavior changes; topology does not.

**What changes vs. initial design:**
- `subscriptions_view` collection: **deleted.**
- `UpdateSubscriptionProjection` activity: **deleted.**
- `internal/store/projection.go`: **deleted.**
- `internal/eventbus/stub.go`: kept as-is — still used for emitting workflow lifecycle events; one of the 10 hooks (`OnActivated`, etc.) replaces this for hook-style dispatch to integration services, but the eventbus stub is repurposed for internal observability logging (not on the hot path).
- `internal/workflow/`: restructured to entity-based methods on a `Subscription` struct.
- `internal/billing/`: **new.** Mongo-backed `BillingEventStore` interface, `Event` model.
- `internal/activity/billing.go`: **new.** `RecordBillingEvent` activity.
- `internal/activity/hooks.go`: **new.** 10 hook-dispatch activities, one per hook name.
- `api/v1/hooks.proto`: **new.** Replaces `integration.proto`; defines `SubscriptionHooks` service with 10 RPC methods.
- Custom search attributes: **new.** Registered at startup; used for `ListSubscriptions` via Visibility.

---

## 5. Lifecycle state machine

5 phases. Each transition fires zero or more hooks (only those the plan opted into via `EnabledHooks`).

### 5.1 Phases

| Phase | Meaning | Search attribute value |
|---|---|---|
| `trialing` | Trial period in effect. No charges yet. Customer can use the service. | `"trialing"` |
| `active` | Paying customer. Current period valid. | `"active"` |
| `past_due` | Most recent renewal charge failed. Workflow is in dunning retry loop. Customer access depends on integrator. | `"past_due"` |
| `canceled` | User requested cancellation. Current period continues running. Workflow does not renew at period end. | `"canceled"` |
| `deactivated` | Terminal. Reached via: cancel + period end, dunning exhausted, trial end without successful charge, or activation charge failed. Workflow run completes. | `"deactivated"` |

### 5.2 Transitions

```
                            (new sub, no trial,
                             activation charge fails)
                                       │
                                       ▼
       (no trial)              ┌──────────────┐
   start ────────────▶ active ─┤              │
                          ▲    │ deactivated  │
       (trial)            │    │   (terminal) │
   start ──▶ trialing ────┤    │              │
                ▲   │     │    └──────────────┘
                │   │     │           ▲
                │   │     │ canceled  │
                │   │     │ + period  │
                │   │     │   end     │
                │   │     │           │
                │   │     │     ┌──────────┐
                │   │     ├────▶│ canceled │
                │   │     │     └──────────┘
                │   │     │           ▲
                │   │     │           │
                │   │     │           │ cancel signal
                │   │     │           │ received
                │   │     │           │
                │   │  renewal charge fails
                │   │     │
                │   │     ▼
                │   │   ┌──────────┐  retries exhausted
                │   │   │ past_due ├──────────────────▶ deactivated
                │   │   └───┬──────┘
                │   │       │ retry succeeds
                │   │       │
                │   │       └────────▶ active
                │   │
                │   │ trial ends, no successful charge / cancel during trial
                │   └─────────────────────────────────▶ deactivated
                │
                │ trial ends, first charge succeeds
                └────────────────────────────────────▶ active
```

### 5.3 Transition → hooks fired

Each transition emits the listed hooks in order, if the plan opted in.

| Transition | Hooks fired |
|---|---|
| start → `trialing` | `subscription.trial_started` |
| `trialing` (at TrialEndNoticeBefore) | `subscription.trial_will_end` |
| `trialing` → `active` (trial ended, first charge OK) | `payment.succeeded`, `subscription.activated` |
| start → `active` (no trial, first charge OK) | `payment.succeeded`, `subscription.activated` |
| `active` → `active` (renewal, charge OK) | `payment.succeeded`, `subscription.renewed` |
| `active` → `past_due` (renewal charge failed) | `payment.failed`, `subscription.past_due` |
| `past_due` → `active` (dunning retry succeeded) | `payment.succeeded`, `subscription.recovered` |
| `past_due` → `deactivated` (dunning exhausted) | `payment.failed`, `subscription.deactivated` |
| `active`/`trialing`/`past_due` → `canceled` (cancel signal) | `subscription.canceled` |
| `canceled` → `deactivated` (period ended) | `subscription.deactivated` |
| `trialing` → `deactivated` (cancel during trial, or trial end + no card) | `subscription.deactivated` |
| start → `deactivated` (activation charge failed at no-trial signup) | `payment.failed`, `subscription.deactivated` |

10 hook names total (each listed exactly once above):

Subscription-level (8):
1. `subscription.trial_started`
2. `subscription.trial_will_end`
3. `subscription.activated`
4. `subscription.renewed`
5. `subscription.past_due`
6. `subscription.recovered`
7. `subscription.canceled`
8. `subscription.deactivated`

Payment-level (2):
9. `payment.succeeded`
10. `payment.failed`

---

## 6. Plan model

```go
package plan

type Plan struct {
    ID                   string         // uuid
    Code                 string         // unique business identifier
    Name                 string
    Cadence              time.Duration  // billing interval ("30d", "1h")
    PriceCents           int64
    Currency             string         // ISO 4217, e.g. "USD"
    PerUserLimit         int            // default 1; 0 = unlimited concurrent

    // Trial
    TrialDuration        time.Duration  // 0 = no trial
    TrialEndNoticeBefore time.Duration  // 0 = no advance notice; e.g. 72h

    // Dunning
    DunningMaxAttempts   int            // 0 = no retries; immediate deactivate on first failure
    DunningRetryBackoff  time.Duration  // initial; doubles each attempt

    // Integration
    IntegrationEndpoint  string         // gRPC host:port; empty = no hooks fire
    EnabledHooks         []string       // subset of the 10 hook names

    CreatedAt            time.Time
}
```

**Defaults applied at `CreatePlan` time:**
- `Currency` = `"USD"`
- `PerUserLimit` = `1`
- `TrialDuration` = `0`
- `TrialEndNoticeBefore` = `0`
- `DunningMaxAttempts` = `3`
- `DunningRetryBackoff` = `24 * time.Hour`
- `EnabledHooks` = `[]` (nothing fires)

**Validation rules:**
- `Code` must be unique (Mongo unique index).
- `Cadence` > 0.
- `PriceCents` ≥ 0 (free plans allowed).
- `TrialEndNoticeBefore` < `TrialDuration` if both set.
- `DunningMaxAttempts` < 100 (sanity cap).
- All entries in `EnabledHooks` must be one of the 10 valid hook names — server rejects unknown names.

---

## 7. Subscription entity

The workflow is implemented as a struct with methods. The exported `SubscriptionWorkflow` function is a thin wrapper for registration; all logic lives on the `Subscription` type.

### 7.1 The entity struct

```go
package workflow

import (
    "time"

    "github.com/martavoi/subflow/internal/domain/subscription"
)

type Phase string

const (
    PhaseStarting    Phase = "starting"
    PhaseTrialing    Phase = "trialing"
    PhaseActive      Phase = "active"
    PhasePastDue     Phase = "past_due"
    PhaseCanceled    Phase = "canceled"
    PhaseDeactivated Phase = "deactivated"
)

type BillingPeriod struct {
    Start time.Time
    End   time.Time
}

// Subscription is the entity workflow aggregate root. Every method is either
// part of the lifecycle (Run, Trial, Renew, ...) or a signal/query/update
// handler. All state lives in fields. No I/O happens here — activities do that.
type Subscription struct {
    // identity (immutable)
    ID       string
    UserID   string
    PlanCode string

    // plan snapshot — captured at start, immutable for this run's behavior
    // (plan changes mid-flight are out of scope for POC; would be handled by
    // re-fetching at CAN boundary in the roadmap version)
    Plan PlanSnapshot

    // dynamic state
    Phase           Phase
    Period          BillingPeriod
    RenewalCount    int
    Context         subscription.Context
    CancelRequested bool

    // dunning state (resets when leaving past_due)
    DunningAttempt int

    // billing summary (O(1), not history)
    LastChargedAt          time.Time
    LastChargeAmountCents  int64
    LastFailureAt          time.Time
    LastFailureReason      string
    TotalChargedCents      int64
    SuccessfulChargeCount  int
    FailedChargeCount      int
}

// PlanSnapshot is the subset of plan fields the workflow needs at runtime.
// Carried in workflow input so the workflow doesn't depend on Mongo.
type PlanSnapshot struct {
    Code                 string
    Cadence              time.Duration
    PriceCents           int64
    Currency             string
    TrialDuration        time.Duration
    TrialEndNoticeBefore time.Duration
    DunningMaxAttempts   int
    DunningRetryBackoff  time.Duration
    IntegrationEndpoint  string
    EnabledHooks         []string
}
```

### 7.2 Lifecycle methods

```go
// Run is the workflow body.
func (s *Subscription) Run(ctx workflow.Context) error {
    if err := s.registerHandlers(ctx); err != nil {
        return err
    }

    if s.RenewalCount == 0 {
        // First period.
        if s.Plan.TrialDuration > 0 {
            // Enter trial phase. May end naturally (→ AwaitActivation) or be
            // canceled mid-trial (→ Deactivate, no charge).
            outcome, err := s.Trial(ctx)
            if err != nil { return err }
            if outcome == trialOutcomeCanceled {
                return s.Deactivate(ctx)
            }
        }
        // First paid period — wait for the Activate update to drive activation.
        if err := s.AwaitActivation(ctx); err != nil {
            return s.Deactivate(ctx)
        }
    } else {
        // Renewal period.
        if err := s.Renew(ctx); err != nil {
            if err := s.HandleDunning(ctx); err != nil {
                return s.Deactivate(ctx)
            }
        }
    }

    if cancelled := s.AwaitPeriodEndOrCancellation(ctx); cancelled {
        return s.Deactivate(ctx)
    }

    return s.ContinueIntoNextPeriod(ctx)
}

// trialOutcome reports how the trial phase ended.
type trialOutcome int

const (
    trialOutcomeEnded    trialOutcome = iota  // trial period elapsed naturally
    trialOutcomeCanceled                       // cancel signal received during trial
)

// Trial parks the workflow in trialing phase until the trial ends or the
// subscription is canceled mid-trial. Returns the trial outcome.
//
// Logic (sketched; full code lives in the plan, not the spec):
//   1. transitionTo(PhaseTrialing); fire subscription.trial_started.
//   2. If TrialEndNoticeBefore > 0, set a timer for (Period.End - TrialEndNoticeBefore).
//      When it fires, fire subscription.trial_will_end hook (best-effort).
//   3. Selector on (trial-end timer | cancel signal observed via s.CancelRequested).
//   4. If cancel observed before trial-end: return trialOutcomeCanceled.
//   5. If trial-end fires first: return trialOutcomeEnded.
func (s *Subscription) Trial(ctx workflow.Context) (trialOutcome, error) {
    // full implementation in the plan
}

// AwaitActivation registers the Activate update handler and blocks on
// workflow.Await until activation completes (handler ran successfully).
func (s *Subscription) AwaitActivation(ctx workflow.Context) error {
    activated := false
    if err := workflow.SetUpdateHandler(ctx, UpdateActivate,
        func(ctx workflow.Context) (ActivationResult, error) {
            if err := s.Activate(ctx); err != nil { return ActivationResult{}, err }
            activated = true
            return ActivationResult{Phase: string(s.Phase), Context: s.Context.Clone()}, nil
        },
    ); err != nil { return err }

    return workflow.Await(ctx, func() bool { return activated })
}

// Activate runs the first-period activation activities: charge, record billing
// event, fire hooks. Internal — invoked from AwaitActivation's update handler
// (no-trial case) and from Trial's end transition (trial → active).
func (s *Subscription) Activate(ctx workflow.Context) error {
    if err := s.charge(ctx, dunningAttempt0); err != nil {
        return err  // surfaces to update caller as FailedPrecondition
    }
    s.transitionTo(ctx, PhaseActive)
    return s.fireHook(ctx, "subscription.activated")
}

// Renew runs the renewal-period activities. Errors propagate up to HandleDunning.
func (s *Subscription) Renew(ctx workflow.Context) error {
    if err := s.charge(ctx, dunningAttempt0); err != nil {
        return err
    }
    s.transitionTo(ctx, PhaseActive)
    return s.fireHook(ctx, "subscription.renewed")
}

// HandleDunning runs the retry loop after a failed renewal charge.
// Transitions to past_due on entry; transitions back to active on recovery;
// returns error if attempts are exhausted.
func (s *Subscription) HandleDunning(ctx workflow.Context) error {
    s.transitionTo(ctx, PhasePastDue)
    if err := s.fireHook(ctx, "subscription.past_due"); err != nil { return err }

    for s.DunningAttempt < s.Plan.DunningMaxAttempts {
        s.DunningAttempt++
        backoff := s.Plan.DunningRetryBackoff * time.Duration(1<<(s.DunningAttempt-1))  // exponential
        if err := workflow.Sleep(ctx, backoff); err != nil { return err }
        if err := s.charge(ctx, s.DunningAttempt); err == nil {
            // recovered
            s.DunningAttempt = 0
            s.transitionTo(ctx, PhaseActive)
            return s.fireHook(ctx, "subscription.recovered")
        }
        // else: loop and try again
    }
    return errors.New("dunning exhausted")
}

// AwaitPeriodEndOrCancellation blocks until the period timer fires or a
// cancel signal arrives. End-of-period semantics retained.
func (s *Subscription) AwaitPeriodEndOrCancellation(ctx workflow.Context) bool {
    /* selector on (period-end timer | cancel signal channel) */
    /* sets s.CancelRequested = true on signal */
    /* returns true if canceled */
}

// Deactivate runs the terminal deactivation activities and returns. After
// this method, the workflow run completes (no CAN).
func (s *Subscription) Deactivate(ctx workflow.Context) error {
    s.transitionTo(ctx, PhaseDeactivated)
    return s.fireHook(ctx, "subscription.deactivated")
}

// ContinueIntoNextPeriod restarts the workflow with the next period's input.
func (s *Subscription) ContinueIntoNextPeriod(ctx workflow.Context) error {
    next := s.nextPeriodInput()
    return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, next)
}
```

### 7.3 Handlers (signal / query / update)

```go
// HandleActivationUpdate is the Activate update entry. Returns synchronously
// to the API caller of UpdateWithStartWorkflow.
func (s *Subscription) HandleActivationUpdate(ctx workflow.Context) (ActivationResult, error) {
    // Already set up inside AwaitActivation; this signature documents the contract.
}

// HandleCancelSignal flips the cancel flag. The main loop reads the flag.
func (s *Subscription) HandleCancelSignal(ctx workflow.Context, _ struct{}) {
    s.CancelRequested = true
    // Don't transition to Canceled here; that happens in AwaitPeriodEnd…
    // or in Trial() when it observes the flag.
}

// HandleContextUpdateSignal merges integrator-supplied context updates.
func (s *Subscription) HandleContextUpdateSignal(ctx workflow.Context, updates map[string]string) {
    if s.Context == nil { s.Context = subscription.Context{} }
    for k, v := range updates { s.Context[k] = v }
}

// AsStatus is the query handler — returns the live subscription snapshot.
func (s *Subscription) AsStatus() (Status, error) {
    return Status{
        Phase: string(s.Phase),
        PeriodStart: s.Period.Start,
        PeriodEnd: s.Period.End,
        RenewalCount: s.RenewalCount,
        Context: s.Context.Clone(),
        CancelRequested: s.CancelRequested,
        LastChargedAt: s.LastChargedAt,
        LastChargeAmountCents: s.LastChargeAmountCents,
        LastFailureAt: s.LastFailureAt,
        LastFailureReason: s.LastFailureReason,
        TotalChargedCents: s.TotalChargedCents,
        SuccessfulChargeCount: s.SuccessfulChargeCount,
        FailedChargeCount: s.FailedChargeCount,
        DunningAttempt: s.DunningAttempt,
    }, nil
}

// registerHandlers wires up query/update/signal handlers at workflow start.
func (s *Subscription) registerHandlers(ctx workflow.Context) error {
    if err := workflow.SetQueryHandler(ctx, QuerySubscriptionStatus, s.AsStatus); err != nil { return err }

    // Signal handlers run in pump goroutines.
    workflow.Go(ctx, func(ctx workflow.Context) {
        ch := workflow.GetSignalChannel(ctx, SignalCancelSubscription)
        for {
            var v struct{}
            ch.Receive(ctx, &v)
            s.HandleCancelSignal(ctx, v)
        }
    })
    workflow.Go(ctx, func(ctx workflow.Context) {
        ch := workflow.GetSignalChannel(ctx, SignalUpdateContext)
        for {
            var updates map[string]string
            ch.Receive(ctx, &updates)
            s.HandleContextUpdateSignal(ctx, updates)
        }
    })

    // The Activate update handler is registered inside AwaitActivation, not here,
    // because it's only valid on first-paid-period.

    return nil
}
```

### 7.4 Internal helpers

```go
// transitionTo updates the phase locally AND upserts the SubflowPhase search
// attribute. Called from each method that crosses a state boundary.
func (s *Subscription) transitionTo(ctx workflow.Context, p Phase) {
    s.Phase = p
    workflow.UpsertSearchAttributes(ctx, map[string]any{
        "SubflowPhase": string(p),
    })
}

// charge runs ChargePayment + RecordBillingEvent in sequence. Always records
// an event (success or failure). Returns the original error.
func (s *Subscription) charge(ctx workflow.Context, dunningAttempt int) error {
    ref := s.activityRef(ctx, fmt.Sprintf("charge:%s:%d", s.phaseTag(), dunningAttempt))
    chargeRes, chargeErr := s.runChargePayment(ctx, ref)

    // Record event regardless of outcome.
    s.recordBillingEvent(ctx, billing.Event{
        ID:             s.eventID(ctx, ref),
        SubscriptionID: s.ID,
        UserID:         s.UserID,
        PlanCode:       s.PlanCode,
        Type:           eventTypeFor(chargeErr),  // "charged" or "failed"
        AmountCents:    s.Plan.PriceCents,
        Currency:       s.Plan.Currency,
        PeriodStart:    s.Period.Start,
        PeriodEnd:      s.Period.End,
        RenewalCount:   s.RenewalCount,
        DunningAttempt: dunningAttempt,
        TransactionID:  chargeRes.TransactionID,
        FailureReason:  errString(chargeErr),
        OccurredAt:     workflow.Now(ctx),
        Reference:      ref,
    })

    // Update summary fields.
    if chargeErr == nil {
        s.LastChargedAt = workflow.Now(ctx)
        s.LastChargeAmountCents = s.Plan.PriceCents
        s.TotalChargedCents += s.Plan.PriceCents
        s.SuccessfulChargeCount++
        // Fire payment.succeeded hook.
        _ = s.fireHook(ctx, "payment.succeeded")
    } else {
        s.LastFailureAt = workflow.Now(ctx)
        s.LastFailureReason = chargeErr.Error()
        s.FailedChargeCount++
        // Fire payment.failed hook.
        _ = s.fireHook(ctx, "payment.failed")
    }

    return chargeErr
}

// fireHook dispatches a single hook activity, but only if (a) the plan
// declares an IntegrationEndpoint and (b) the hook is listed in EnabledHooks.
// No-op otherwise. Errors from hook activities are non-fatal — log and continue.
func (s *Subscription) fireHook(ctx workflow.Context, hookName string) error {
    if s.Plan.IntegrationEndpoint == "" { return nil }
    if !contains(s.Plan.EnabledHooks, hookName) { return nil }

    // Each hook name maps to a distinct activity name. The activity is
    // responsible for making the gRPC call to s.Plan.IntegrationEndpoint.
    activityName := hookActivityName(hookName)
    opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        RetryPolicy:         activity.HookRetry,
    })
    return workflow.ExecuteActivity(opts, activityName, s.hookEvent(hookName)).Get(ctx, nil)
}

// activityRef builds the idempotency token for an activity call.
func (s *Subscription) activityRef(ctx workflow.Context, suffix string) string {
    info := workflow.GetInfo(ctx)
    return fmt.Sprintf("%s:%s:%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, suffix)
}
```

### 7.5 Registration wrapper

```go
// SubscriptionWorkflow is the top-level workflow function registered with the worker.
// It constructs a Subscription entity from input and runs it.
func SubscriptionWorkflow(ctx workflow.Context, in SubscriptionInput) error {
    return NewSubscription(in).Run(ctx)
}

// Worker registration:
//   w.RegisterWorkflow(workflow.SubscriptionWorkflow)
```

### 7.6 File layout

```
internal/workflow/
├── subscription.go       // type Subscription + NewSubscription + SubscriptionWorkflow + Run
├── trial.go              // method: Trial
├── activation.go         // methods: AwaitActivation, Activate, HandleActivationUpdate
├── renewal.go            // method: Renew
├── dunning.go            // method: HandleDunning
├── cancellation.go       // methods: AwaitPeriodEndOrCancellation, Deactivate
├── continuation.go       // method: ContinueIntoNextPeriod
├── handlers.go           // methods: HandleCancelSignal, HandleContextUpdateSignal, AsStatus
├── hooks.go              // methods: fireHook, hookEvent, hookActivityName
├── helpers.go            // methods: transitionTo, charge, recordBillingEvent, activityRef
├── signals.go            // signal/query/update name constants
├── status.go             // type Status (query result type)
└── subscription_test.go  // testsuite tests
```

---

## 8. Workflow primitives

### 8.1 Signals (sent from API)

| Name | Payload | Purpose |
|---|---|---|
| `subscription.cancel` | `struct{}` | User requests cancellation. End-of-period semantics. |
| `subscription.update_context` | `map[string]string` | Integrator updates the per-subscription context bag. |

### 8.2 Queries

| Name | Return type | Purpose |
|---|---|---|
| `subscription.status` | `workflow.Status` | Live snapshot of the entity. |

### 8.3 Updates

| Name | Args | Return | Purpose |
|---|---|---|---|
| `subscription.activate` | `()` | `(ActivationResult, error)` | Sent by API immediately after workflow start (via `UpdateWithStartWorkflow`). Runs activation activities synchronously. |

### 8.4 Search attributes

Custom search attributes registered once at server startup via `temporal operator search-attribute create`:

| Name | Type | Lifecycle |
|---|---|---|
| `SubflowUserId` | `Keyword` | Set via `StartWorkflowOptions.SearchAttributes` at creation. Never changes. Auto-carried across CAN. |
| `SubflowPlanCode` | `Keyword` | Set via `StartWorkflowOptions`. Never changes. Auto-carried across CAN. |
| `SubflowPhase` | `Keyword` | Set initial via `StartWorkflowOptions`. Upserted from `transitionTo()` on each state change. Auto-carried across CAN unless updated. |
| `SubflowPeriodEnd` | `Datetime` | Set initial via `StartWorkflowOptions`. Upserted at start of every renewal CAN run (because the value changes per period). |
| `SubflowTrialEnd` | `Datetime` | Set via `StartWorkflowOptions` only when `TrialDuration > 0`. Removed (set to zero-value) when transitioning out of trialing — Temporal has no remove, so we use a sentinel zero time and filter `SubflowTrialEnd > 0`. |

Registration is automated at API server startup via a small bootstrapper using `OperatorService.AddSearchAttributes`. Failures (e.g. attribute already exists) are logged and ignored.

### 8.5 Listing via Visibility

The `ListSubscriptions` handler constructs a Visibility query and calls `client.ListWorkflow`:

```go
parts := []string{"WorkflowType='SubscriptionWorkflow'"}
if req.UserId != ""   { parts = append(parts, fmt.Sprintf("SubflowUserId='%s'", req.UserId)) }
if req.PlanCode != "" { parts = append(parts, fmt.Sprintf("SubflowPlanCode='%s'", req.PlanCode)) }
if req.Phase != ""    { parts = append(parts, fmt.Sprintf("SubflowPhase='%s'", req.Phase)) }
query := strings.Join(parts, " AND ")

resp, err := s.Temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
    Namespace: s.Namespace,
    Query:     query,
    PageSize:  int32(req.PageSize),
    NextPageToken: req.PageToken,
})
```

Each returned execution is then queried via `QueryWorkflow(.., QuerySubscriptionStatus)` to get current state. For larger result sets, this is N+1 queries — acceptable for POC; production would use a search-attribute-rich enough projection to skip the N+1, or accept eventual-consistency lag from Visibility-only data.

The `CountWorkflow` API is used by the per-user-limit check at create-time:
```go
count, err := s.Temporal.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
    Namespace: s.Namespace,
    Query:     fmt.Sprintf(
        "WorkflowType='SubscriptionWorkflow' AND SubflowUserId='%s' AND SubflowPlanCode='%s' AND SubflowPhase IN ('trialing','active','past_due','canceled')",
        req.UserId, plan.Code,
    ),
})
if plan.PerUserLimit > 0 && count.Count >= int64(plan.PerUserLimit) {
    return nil, status.Errorf(codes.FailedPrecondition, "per-user limit reached for plan %q", plan.Code)
}
```

---

## 9. Hooks integration contract

`api/v1/hooks.proto` defines a 10-method gRPC service. The integration service (subflow's mock or any real integration) implements only the hooks it cares about.

```protobuf
syntax = "proto3";
package subflow.hooks.v1;
option go_package = "github.com/martavoi/subflow/api/v1;subflowv1";

import "google/protobuf/timestamp.proto";

service SubscriptionHooks {
  rpc OnTrialStarted     (LifecycleEvent) returns (HookAck);
  rpc OnTrialWillEnd     (LifecycleEvent) returns (HookAck);
  rpc OnActivated        (LifecycleEvent) returns (HookAck);
  rpc OnRenewed          (LifecycleEvent) returns (HookAck);
  rpc OnPastDue          (LifecycleEvent) returns (HookAck);
  rpc OnRecovered        (LifecycleEvent) returns (HookAck);
  rpc OnCanceled         (LifecycleEvent) returns (HookAck);
  rpc OnDeactivated      (LifecycleEvent) returns (HookAck);

  rpc OnPaymentSucceeded (PaymentEvent)   returns (HookAck);
  rpc OnPaymentFailed    (PaymentEvent)   returns (HookAck);
}

message LifecycleEvent {
  string reference = 1;
  string subscription_id = 2;
  string user_id = 3;
  string plan_code = 4;
  string phase = 5;
  int32 renewal_count = 6;
  google.protobuf.Timestamp period_start = 7;
  google.protobuf.Timestamp period_end = 8;
  google.protobuf.Timestamp event_time = 9;
  map<string,string> context = 10;
}

message PaymentEvent {
  string reference = 1;
  string subscription_id = 2;
  string user_id = 3;
  string plan_code = 4;
  int32 renewal_count = 5;
  int32 dunning_attempt = 6;
  int64 amount_cents = 7;
  string currency = 8;
  string transaction_id = 9;
  string failure_reason = 10;
  google.protobuf.Timestamp event_time = 11;
  map<string,string> context = 12;
}

message HookAck {
  // Intentionally empty. Pure notification — no flow control, no context updates.
  // Integrators that need to mutate subscription state call
  // SubflowService.UpdateSubscriptionContext.
}
```

**Hook activity dispatch:** one Temporal activity per hook name (10 activities). Each activity dials the plan's `IntegrationEndpoint`, calls the matching RPC, returns. Failures are retried per `HookRetry` policy (gentle, unlimited — events should eventually arrive). Failures don't block the workflow's main lifecycle.

```go
// internal/activity/hooks.go
type HookActivities struct {
    Client *integration.Client  // pooled gRPC connections
}

func (a *HookActivities) OnActivated(ctx context.Context, ev LifecycleEvent) error {
    stub, err := a.Client.HooksStub(ev.IntegrationEndpoint)
    if err != nil { return err }
    _, err = stub.OnActivated(ctx, toProto(ev))
    return err
}
// ... 9 more similar methods
```

Activity registration:
```go
w.RegisterActivityWithOptions(hookActs.OnActivated,    tactivity.RegisterOptions{Name: "hook.OnActivated"})
w.RegisterActivityWithOptions(hookActs.OnRenewed,      tactivity.RegisterOptions{Name: "hook.OnRenewed"})
// ... etc for all 10
```

The workflow refers to them by name (`workflow.ExecuteActivity(opts, "hook.OnActivated", ev)`), matching what the worker registered.

---

## 10. Billing events store

### 10.1 Event model

```go
// internal/billing/event.go
package billing

type Event struct {
    ID             string         // primary key; deterministic from workflow context
    SubscriptionID string
    UserID         string
    PlanCode       string
    Type           string         // "charged" | "failed" | "refunded" (refunded reserved for roadmap)
    AmountCents    int64
    Currency       string
    PeriodStart    time.Time
    PeriodEnd      time.Time
    RenewalCount   int
    DunningAttempt int            // 0 on first attempt, 1+ on dunning retries
    TransactionID  string         // empty on failure
    FailureReason  string         // empty on success
    OccurredAt     time.Time
    Reference      string         // idempotency token = activityRef
}

type ListQuery struct {
    SubscriptionID string         // optional filter
    UserID         string         // optional filter
    TypeFilter     string         // optional: "charged" | "failed" | "refunded"
    PageSize       int            // default 50, max 200
    PageCursor     string         // opaque, occurred_at-based
}
```

### 10.2 Store interface

```go
// internal/billing/store.go
type EventStore interface {
    Append(ctx context.Context, ev Event) error
    List(ctx context.Context, q ListQuery) (events []Event, nextCursor string, err error)
}
```

### 10.3 Mongo implementation

```go
// internal/billing/mongo_store.go
type MongoStore struct {
    col *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
    return &MongoStore{col: db.Collection("billing_events")}
}

func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
    _, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
        {Keys: bson.D{{Key: "reference", Value: 1}}, Options: options.Index().SetUnique(true).SetName("reference_unique")},
        {Keys: bson.D{{Key: "subscription_id", Value: 1}, {Key: "occurred_at", Value: -1}}, Options: options.Index().SetName("sub_occurred")},
        {Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "occurred_at", Value: -1}}, Options: options.Index().SetName("user_occurred")},
    })
    return err
}

func (s *MongoStore) Append(ctx context.Context, ev Event) error {
    _, err := s.col.InsertOne(ctx, ev)
    if mongo.IsDuplicateKeyError(err) {
        return nil  // already written — idempotent
    }
    return err
}

func (s *MongoStore) List(ctx context.Context, q ListQuery) ([]Event, string, error) {
    // filter + sort by occurred_at DESC, paginate with cursor
}
```

### 10.4 Activity

```go
// internal/activity/billing.go
type BillingActivities struct {
    Events billing.EventStore
}

func (a *BillingActivities) RecordBillingEvent(ctx context.Context, ev billing.Event) error {
    return a.Events.Append(ctx, ev)
}

var BillingEventRetry = &temporal.RetryPolicy{
    InitialInterval:    time.Second,
    BackoffCoefficient: 2.0,
    MaximumInterval:    time.Minute,
    MaximumAttempts:    20,  // ~20 min total tolerance
}
```

Bounded retries — beyond 20 attempts, the activity fails terminally, the workflow logs an error, but the charge already happened (workflow history retains the activity input/output for forensic recovery). For POC this is acceptable.

### 10.5 API listing

```protobuf
rpc ListBillingEvents(ListBillingEventsRequest) returns (ListBillingEventsResponse);

message ListBillingEventsRequest {
  string subscription_id = 1;
  string user_id = 2;
  string type_filter = 3;        // optional: "charged" | "failed" | "refunded"
  int32  page_size = 4;
  string page_cursor = 5;
}

message ListBillingEventsResponse {
  repeated BillingEventProto events = 1;
  string next_cursor = 2;
}
```

The handler reads directly from `billing.EventStore` — workflow not involved.

### 10.6 Future Kafka path

The `EventStore` interface lets the implementation be swapped:

```go
// future: internal/billing/kafka_store.go
type KafkaStore struct {
    producer kafka.Producer
    readModel *MongoStore   // consumer-materialized view for List()
}
```

`Append` publishes to a topic; `List` reads from a Mongo-backed materialized view that a separate consumer service populates. Workflow code unchanged.

---

## 11. gRPC API surface

```protobuf
service SubflowService {
  // Plans (unchanged in shape; Plan message gets new fields)
  rpc CreatePlan(CreatePlanRequest)        returns (Plan);
  rpc GetPlan(GetPlanRequest)              returns (Plan);
  rpc ListPlans(ListPlansRequest)          returns (ListPlansResponse);
  rpc DeletePlan(DeletePlanRequest)        returns (DeletePlanResponse);

  // Subscriptions
  rpc CreateSubscription(CreateSubscriptionRequest)           returns (Subscription);
  rpc CancelSubscription(CancelSubscriptionRequest)           returns (CancelSubscriptionResponse);
  rpc GetSubscription(GetSubscriptionRequest)                 returns (Subscription);
  rpc ListSubscriptions(ListSubscriptionsRequest)             returns (ListSubscriptionsResponse);
  rpc UpdateSubscriptionContext(UpdateSubscriptionContextRequest) returns (Subscription);  // NEW
  rpc ListBillingEvents(ListBillingEventsRequest)             returns (ListBillingEventsResponse);  // NEW
}
```

### Plan message (revised)

```protobuf
message Plan {
  string id = 1;
  string code = 2;
  string name = 3;
  string cadence = 4;                         // Go duration: "30s" / "720h"
  int64 price_cents = 5;
  string currency = 6;
  int32 per_user_limit = 7;                   // default 1, 0 = unlimited
  string trial_duration = 8;                  // Go duration; empty = no trial
  string trial_end_notice_before = 9;         // Go duration; empty = no notice
  int32 dunning_max_attempts = 10;
  string dunning_retry_backoff = 11;          // Go duration; initial; doubles each attempt
  string integration_endpoint = 12;
  repeated string enabled_hooks = 13;
}
```

### Subscription message (revised — includes billing summary)

```protobuf
message Subscription {
  string id = 1;
  string user_id = 2;
  string plan_code = 3;
  string phase = 4;
  google.protobuf.Timestamp period_start = 5;
  google.protobuf.Timestamp period_end = 6;
  int32 renewal_count = 7;
  map<string,string> context = 8;
  bool cancel_requested = 9;

  // billing summary (NEW)
  int32 dunning_attempt = 10;
  google.protobuf.Timestamp last_charged_at = 11;
  int64 last_charge_amount_cents = 12;
  google.protobuf.Timestamp last_failure_at = 13;
  string last_failure_reason = 14;
  int64 total_charged_cents = 15;
  int32 successful_charge_count = 16;
  int32 failed_charge_count = 17;
}
```

### Handler translation table

| RPC | Translation |
|---|---|
| `CreatePlan / Get / List / Delete` | Mongo `plans` CRUD |
| `CreateSubscription` | Look up plan → per-user-limit check via `CountWorkflow` → `UpdateWithStartWorkflow(SubscriptionWorkflow, ..., update=subscription.activate)` → return result (phase `trialing` or `active` or FailedPrecondition) |
| `CancelSubscription` | `SignalWorkflow("subscription.cancel")` |
| `GetSubscription` | `QueryWorkflow("subscription.status")`; fall back to "not found" if execution closed and beyond retention |
| `ListSubscriptions` | `ListWorkflow` with Visibility query built from filter args |
| `UpdateSubscriptionContext` | `SignalWorkflow("subscription.update_context", updates)` |
| `ListBillingEvents` | `billing.EventStore.List(...)` directly — no workflow involvement |

---

## 12. Persistence layout

### Mongo

| Collection | Purpose | Index summary |
|---|---|---|
| `plans` | Static plan config | unique on `code` |
| `billing_events` | Append-only charge history | unique on `reference`, compound on `(subscription_id, occurred_at DESC)`, compound on `(user_id, occurred_at DESC)` |

No `subscriptions_view`. No subscription state in Mongo at all.

### Temporal (SQLite dev server)

- Workflow histories (built-in)
- Custom search attributes (registered at startup)
- Visibility index (SQLite-backed; sufficient for POC scale)

---

## 13. Error handling and retries

### Retry policies (all in `internal/activity/retry.go`)

```go
var ChargePaymentRetry = &temporal.RetryPolicy{
    InitialInterval:    time.Second,
    BackoffCoefficient: 2.0,
    MaximumInterval:    time.Minute,
    MaximumAttempts:    5,
    NonRetryableErrorTypes: []string{
        "InsufficientFundsError",
        "CardDeclinedError",
    },
}

var BillingEventRetry = &temporal.RetryPolicy{
    InitialInterval:    time.Second,
    BackoffCoefficient: 2.0,
    MaximumInterval:    time.Minute,
    MaximumAttempts:    20,  // ~20 min tolerance
}

var HookRetry = &temporal.RetryPolicy{
    InitialInterval:    time.Second,
    BackoffCoefficient: 2.0,
    MaximumInterval:    5 * time.Minute,
    MaximumAttempts:    0,  // unlimited — hooks must eventually deliver
    NonRetryableErrorTypes: []string{
        "HookTerminalError",
    },
}
```

### Failure semantics

| Scenario | Behavior |
|---|---|
| Activation charge fails terminally (card declined) | Update returns error → API maps to `FailedPrecondition` → subscription transitions to `deactivated` and workflow ends |
| Renewal charge fails terminally | Enter `past_due` → dunning loop → if exhausted, `deactivated`; if recovered, `active` |
| Hook activity fails | Retried per `HookRetry`. Workflow's main lifecycle is not blocked because hooks are fired sequentially after primary state transitions (state already updated before hook fires). |
| Billing event write fails 20 times | Workflow logs critical error, continues. Charge already happened — workflow history is the fallback audit trail. |
| Worker dies mid-period | Temporal reschedules, replay reconstructs entity state from history. Idempotency tokens prevent double-charge. |
| Mock-integration down | All hook activities retry. Lifecycle progresses (state changes happen before hooks). |

### Dunning details

When a renewal charge fails (non-retryable error or all `ChargePaymentRetry` attempts exhausted), the workflow enters `past_due`:

1. `transitionTo(PhasePastDue)` → upserts SA
2. Fire `subscription.past_due` hook (best-effort)
3. Loop until `DunningMaxAttempts` reached:
   - Sleep `DunningRetryBackoff * 2^(attempt-1)` (durable timer)
   - `charge()` → record event, fire payment hooks
   - On success: `transitionTo(PhaseActive)`, fire `subscription.recovered`, reset `DunningAttempt = 0`, return nil
   - On failure: increment `DunningAttempt`, continue
4. Exhausted: return error → caller transitions to `deactivated`

A cancel signal during `past_due` is honored — the workflow exits the dunning loop, transitions to `canceled`, runs to "end of period" (the original `Period.End`), then `deactivates`.

---

## 14. Testing strategy

### Layers

| Layer | What | How |
|---|---|---|
| Pure domain | `NextBillingPeriod`, `Plan` validation | Go table tests |
| Activities | `ChargePayment`, `RecordBillingEvent`, hook activities | Direct Go tests with interface mocks (Mongo + gRPC) |
| Subscription methods (pure) | `transitionTo`, `phaseTag`, `nextPeriodInput` | Direct Go tests, no workflow context needed |
| Workflow lifecycle | `Run` with mocked activities | `temporal.io/sdk/testsuite` with `RegisterActivityWithOptions(name, mockFunc)` |
| Listing | `ListSubscriptions` Visibility query construction | Direct Go tests asserting the query string |
| Manual / e2e | Full happy path + cancel + dunning | `task up` + grpcurl flows in README |

### Required workflow tests

1. **Happy path no trial:** activate → first renewal succeeds → CAN to next period → cancel mid-second-period → deactivate. Assert `subscription.activated`, `subscription.renewed`, `payment.succeeded` ×2, `subscription.canceled`, `subscription.deactivated` hooks fire.

2. **Trial → active:** trialing → trial ends → first charge succeeds → active. Assert `subscription.trial_started`, `subscription.trial_will_end` (if notice configured), `payment.succeeded`, `subscription.activated`.

3. **Trial canceled mid-trial:** trialing → cancel → deactivate (no charge). Assert no `payment.*` hooks fire.

4. **Renewal failure → dunning → recovery:** active → renewal charge fails → past_due → retry succeeds → active. Assert `payment.failed`, `subscription.past_due`, `payment.succeeded`, `subscription.recovered`.

5. **Renewal failure → dunning exhausted:** active → renewal fails repeatedly → dunning exhausted → deactivate. Assert `subscription.deactivated`.

6. **Per-user limit:** `CreateSubscription` rejected with `FailedPrecondition` when `CountWorkflow` returns ≥ limit. (Tested at the handler level, not the workflow.)

### Mocking pattern

The Subscription entity has methods that take `workflow.Context` and call activities by name. In tests we register mock activity implementations under the same names:

```go
env.RegisterActivityWithOptions(
    func(ctx context.Context, in ChargePaymentInput) (ChargePaymentResult, error) {
        // mock logic; can return error to simulate failure
    },
    activity.RegisterOptions{Name: "ChargePayment"},
)
```

For hook activities, register all 10 (or only those the test cares about) with observers that record which hooks fired.

---

## 15. Migration plan (what gets ripped out, what gets added)

### Deleted

| Path | Reason |
|---|---|
| `internal/store/projection.go` | Replaced by Visibility |
| `internal/activity/projection.go` | No projection to update |
| `internal/eventbus/stub.go` Publisher invocations from workflow | Hook activities replace this on the main path |
| `cmd/api/main.go` projection wiring | Not needed |
| `cmd/worker/main.go` projection activity registration | Not needed |
| `api/v1/integration.proto` | Replaced by `hooks.proto` |
| Generated `integration*.pb.go` | Regenerated from new proto |

### New

| Path | Contents |
|---|---|
| `internal/workflow/` | Restructured per §7.6 (12 files) |
| `internal/billing/event.go` | `Event` + `ListQuery` types |
| `internal/billing/store.go` | `EventStore` interface |
| `internal/billing/mongo_store.go` | Mongo-backed implementation |
| `internal/activity/billing.go` | `BillingActivities.RecordBillingEvent` |
| `internal/activity/hooks.go` | 10 hook activities |
| `internal/integration/client.go` | Updated for `SubscriptionHooks` service |
| `internal/temporal/search_attributes.go` | SA constants + bootstrapper |
| `api/v1/hooks.proto` | Replaces `integration.proto` |

### Modified

| Path | Change |
|---|---|
| `internal/domain/plan/plan.go` | Add new fields (Cadence, PerUserLimit, TrialDuration, etc.) |
| `internal/store/plans.go` | New fields in `planDoc` and `docToPlan` |
| `internal/server/plans.go` | Validate new fields + apply defaults |
| `internal/server/subscriptions.go` | `ListSubscriptions` via Visibility; new `UpdateSubscriptionContext` and `ListBillingEvents` handlers |
| `cmd/api/main.go` | Register search attributes at startup; wire billing event store |
| `cmd/worker/main.go` | Register hook activities; register `RecordBillingEvent`; remove `UpdateSubscriptionProjection` registration |
| `cmd/mock-integration/main.go` | Implement all 10 `SubscriptionHooks` methods (no-ops with optional failure injection per method) |
| `api/v1/subflow.proto` | Plan + Subscription messages get new fields; new RPCs `UpdateSubscriptionContext` and `ListBillingEvents` |
| `compose.yml` | No structural changes; consider env var for SA bootstrapping policy |
| `README.md` | Document new state machine, hooks, billing events |
| `docs/architecture.md` | Update diagram |
| `docs/temporal-cheatsheet.md` | Add search attributes, UpsertSearchAttributes, dunning loop |

### Approach

This is a hard reset of the workflow + storage layers. The redesign branch (`redesign/temporal-native`) starts from `main` and replaces files wholesale. The previous design is preserved in `main`'s history and in `docs/superpowers/specs/2026-05-09-subflow-design.md` for reference.

---

## 16. Open questions

None blocking. Items deferred to roadmap rather than POC scope are listed in §3.
