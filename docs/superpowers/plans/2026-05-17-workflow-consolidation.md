# Workflow Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the five decisions in `docs/superpowers/specs/2026-05-17-workflow-consolidation-design.md` — rename Status→View, drop `plan.Snapshot`, fold `internal/activity` into `internal/workflow`, introduce `hook.Event` integration-event contract, split phases by file — backed by four ADRs and a CONTEXT.md update.

**Architecture:** Pure refactor. No behavior changes. Tests must stay green across every task. Each task ends with `go build ./...` and `go test ./...` both passing, then a single commit. Tasks are ordered to minimise cross-task code thrash: ADRs first (intent before code), then leaf renames (`plan.Snapshot` drop, Status→View), then the structural moves (fold activity package, hook redesign, phase split), then docs.

**Tech Stack:** Go 1.22+, Temporal Go SDK, MongoDB driver, gRPC/Protobuf (`api/v1/*.proto`).

**Spec sections covered:** D1 → Task 4. D2 → Task 2. D3 → Task 3. D4 → Tasks 5 & 6. D5 → Task 7. ADRs → Task 1. CONTEXT.md → Task 8. Verification gate → Task 9.

---

## Task 1: Write the four ADRs

**Files:**
- Create: `docs/adr/0004-rename-status-to-view.md`
- Create: `docs/adr/0005-drop-plan-snapshot.md`
- Create: `docs/adr/0006-fold-activity-into-workflow-package.md`
- Create: `docs/adr/0007-integration-event-contract.md`

ADRs document intent before code. Style matches existing ADRs (`docs/adr/0001..0003`): a title that is the decision, short `## Why`, optional `## Considered`, optional `## Consequences`.

- [ ] **Step 1: Create `docs/adr/0004-rename-status-to-view.md`**

```markdown
# Rename Status to View

The query previously named `subscription.status` returns a read-model snapshot of the subscription for API rendering. Rename type `workflow.Status` to `workflow.View`, query name to `subscription.view`, constant to `QuerySubscriptionView`, handler method to `(*Subscription).View()`.

## Why

`CONTEXT.md` lists "Status" under `_Avoid_` for `Phase`. Using "Status" both for a synonym of the lifecycle state and for the query return type conflated two concepts. The thing the query returns is read-model data (current phase, period boundaries, charge counters, context) — not a lifecycle state. "View" is the CQRS-shaped term for a read-model projection and aligns cleanly with how the API consumes it.

## Consequences

The `Phase` enum and its values are unchanged. The `subscription.view` glossary entry is added to `CONTEXT.md` so future readers know the read-model term.
```

- [ ] **Step 2: Create `docs/adr/0005-drop-plan-snapshot.md`**

```markdown
# Drop plan.Snapshot

The workflow input carries `plan.Plan` directly. The `plan.Snapshot` type and `plan.SnapshotOf` constructor are removed.

## Why

`Snapshot` existed to expose only the workflow-relevant subset of `Plan` and to hide persistence-only fields (`ID`, `Name`, `PerUserLimit`, `CreatedAt`) from workflow code. In practice the cost (parallel struct, parallel constructor, parallel test fixtures) exceeded the value (saving four fields of memory and a comment). The workflow only reads the fields it needs from `plan.Plan`; the extra fields ride along unused.

## Consequences

Adding a new plan field is one place to update instead of two. Workflow input is slightly larger but unchanged in shape on the wire.
```

- [ ] **Step 3: Create `docs/adr/0006-fold-activity-into-workflow-package.md`**

```markdown
# Fold internal/activity into internal/workflow

The package `internal/activity` is removed. Its files move into `internal/workflow` with an `activity_` filename prefix, matching the existing `phase_` convention. The Temporal worker registers handlers from a single `workflow` package.

## Why

Activities and workflows live in the same Go package in canonical Temporal Go references (`temporalio/samples-go`, the official ecommerce and expense tutorials). Subflow's split added an import boundary without paying back: workflows still depended on the activity package by name, the activity package still depended on `internal/billing` and `internal/integration`, and the two packages always shipped together.

## Consequences

The `activity_` filename prefix is the only convention separating activity handlers from workflow code. Go has no language-level guard against calling an activity handler function directly from a workflow function (which would silently execute non-deterministically). Code review and the filename hint are the safeguards.
```

- [ ] **Step 4: Create `docs/adr/0007-integration-event-contract.md`**

```markdown
# Integration-event contract: hook.Event with sealed payload

The integration-event payload published to integrators is `hook.Event` — a canonical struct carrying common identity/context fields and a sealed-interface `hook.Payload`. Payload variants are `hook.LifecyclePayload` and `hook.PaymentPayload` today; new variants are additive.

## Why

The previous design had two near-identical workflow methods (`fireLifecycle`, `firePayment`) plus two nullable pointers (`*LifecycleData`, `*PaymentData`) on the activity input. Adding a new payload category would have meant another nullable pointer, another switch arm, and another near-duplicate emit function. The sealed interface makes the payload required and typed: workflow method count grows with payload categories (two today) rather than hook types (eleven today).

## Considered

- **Kafka as the integration substrate.** Out of scope for this stage. Reporting and pub-sub fan-out can come later via a Kafka producer activity without re-shaping `hook.Event`.
- **One method per hook type** (`emitTrialStarted`, `emitRenewed`, ...). Rejected: eleven methods that mostly duplicate boilerplate.
- **One emit method with `any` payload.** Rejected: payload could be any type, losing compile-time guarantees that integrator-facing payloads contain only known shapes.
- **Saga compensation helpers.** Subflow has no cross-activity transactions to compensate (per ADR 0002 hooks are pure-notification, per ADR 0003 billing-event writes are log-and-continue). Not adopted.

## Consequences

Hook events are **integration events**, not domain events in the DDD sense. They are published outward; the workflow does not consume its own emissions to drive internal state. The Subscription aggregate (per ADR 0001) drives state changes directly through method calls, not through an internal event bus.
```

- [ ] **Step 5: Verify build still green**

Run: `go build ./... && go test ./...`
Expected: no output, exit 0 (ADRs are docs only).

- [ ] **Step 6: Commit**

```bash
git add docs/adr/0004-rename-status-to-view.md docs/adr/0005-drop-plan-snapshot.md docs/adr/0006-fold-activity-into-workflow-package.md docs/adr/0007-integration-event-contract.md
git commit -m "$(cat <<'EOF'
docs(adr): record decisions for workflow consolidation refactor

0004 rename Status to View
0005 drop plan.Snapshot
0006 fold internal/activity into internal/workflow
0007 integration-event contract (hook.Event with sealed Payload)
EOF
)"
```

---

## Task 2: Drop `plan.Snapshot`

**Files:**
- Modify: `internal/domain/plan/plan.go`
- Modify: `internal/workflow/input.go`
- Modify: `internal/workflow/subscription.go`
- Modify: `internal/workflow/subscription_test.go`
- Modify: `internal/workflow/input_test.go`
- Modify: `internal/server/subscriptions.go`

Background: `plan.Snapshot` is the workflow-carried subset of `plan.Plan`. After this task, the workflow carries `plan.Plan` directly. The five extra fields (`ID`, `Name`, `PerUserLimit`, `CreatedAt`) ride along unused — explicitly so.

- [ ] **Step 1: Delete `Snapshot` and `SnapshotOf` from `internal/domain/plan/plan.go`**

Edit the file so it contains only the `Plan` struct. Final contents:

```go
package plan

import (
	"time"

	"github.com/martavoi/subflow/internal/hook"
)

// Plan is a subscription plan aggregate. Persisted in the plans collection.
type Plan struct {
	ID                    string
	Code                  string
	Name                  string
	Cadence               time.Duration
	PriceCents            int64
	Currency              string
	PerUserLimit          int // 0 = unlimited
	TrialDuration         time.Duration
	TrialEndNoticeBefore  time.Duration
	RenewalUpcomingBefore time.Duration
	DunningMaxAttempts    int
	DunningRetryBackoff   time.Duration
	IntegrationEndpoint   string
	EnabledHooks          []hook.Type
	CreatedAt             time.Time
}
```

- [ ] **Step 2: Update `internal/workflow/input.go`**

Change the `Plan` field type from `plan.Snapshot` to `plan.Plan`. Final field line in `SubscriptionInput`:

```go
	Plan            plan.Plan
```

No other changes in this file — `NextBillingPeriod` already reads only the fields it needs.

- [ ] **Step 3: Update `internal/workflow/subscription.go`**

Change the `Plan` field type from `plan.Snapshot` to `plan.Plan` in the `Subscription` struct. Final field line:

```go
	Plan plan.Plan
```

`NewSubscription` already copies `in.Plan` by value; no change needed there.

- [ ] **Step 4: Update test helper in `internal/workflow/subscription_test.go`**

Change `sampleInput`'s mods callback type from `func(*plan.Snapshot)` to `func(*plan.Plan)`, and the local `p := plan.Snapshot{...}` to `p := plan.Plan{...}`. Replace every `func(p *plan.Snapshot)` in the file (there are seven test functions using this pattern) with `func(p *plan.Plan)`.

The relevant lines in `sampleInput`:

```go
func sampleInput(mods ...func(*plan.Plan)) SubscriptionInput {
	start := time.Now().UTC().Truncate(time.Second)
	p := plan.Plan{
		Code:                "monthly-basic",
		Cadence:             30 * 24 * time.Hour,
		PriceCents:          999,
		Currency:            "USD",
		IntegrationEndpoint: "mock:50052",
		EnabledHooks: []hook.Type{
			hook.TrialStarted, hook.TrialWillEnd,
			hook.RenewalUpcoming,
			hook.Activated, hook.Renewed,
			hook.PastDue, hook.Recovered,
			hook.Canceled, hook.Deactivated,
			hook.PaymentOK, hook.PaymentFailed,
		},
	}
	// ... rest unchanged
}
```

Use `grep -n "plan.Snapshot" internal/workflow/subscription_test.go` first to enumerate every line that needs editing.

- [ ] **Step 5: Update `internal/workflow/input_test.go`**

Same swap: any `plan.Snapshot{...}` literal becomes `plan.Plan{...}`. Confirm with `grep -n "plan.Snapshot" internal/workflow/input_test.go`.

- [ ] **Step 6: Update `internal/server/subscriptions.go` call site**

At line 67 the file currently has `Plan: plan.SnapshotOf(p),`. Replace with `Plan: p,`:

```go
	wfInput := workflow.SubscriptionInput{
		SubscriptionID: subID,
		IntervalID:     uuid.NewString(),
		UserID:         req.UserId,
		PlanID:         p.ID,
		Plan:           p,
		PeriodStart:    now,
		PeriodEnd:      periodEnd,
		Context:        workflow.Context(req.InitialContext),
	}
```

- [ ] **Step 7: Verify**

Run: `go build ./... && go test ./...`
Expected: no errors, all tests pass.

If a `plan.Snapshot` reference is missed, the compiler will name the file and line. Use `grep -rn "plan\.Snapshot\|SnapshotOf" .` from repo root — should return zero results before continuing.

- [ ] **Step 8: Commit**

```bash
git add internal/domain/plan/plan.go internal/workflow/input.go internal/workflow/subscription.go internal/workflow/subscription_test.go internal/workflow/input_test.go internal/server/subscriptions.go
git commit -m "$(cat <<'EOF'
refactor(workflow): drop plan.Snapshot, carry full plan.Plan

See docs/adr/0005-drop-plan-snapshot.md.
EOF
)"
```

---

## Task 3: Fold `internal/activity` into `internal/workflow`

**Files:**
- Move with `git mv`: `internal/activity/payment.go` → `internal/workflow/activity_payment.go`
- Move with `git mv`: `internal/activity/billing.go` → `internal/workflow/activity_billing.go`
- Move with `git mv`: `internal/activity/hooks.go` → `internal/workflow/activity_hook.go`
- Move with `git mv`: `internal/activity/retry.go` → `internal/workflow/retry.go`
- Move with `git mv`: `internal/activity/errors.go` → `internal/workflow/errors.go`
- Modify (package decl): all five moved files.
- Modify: `internal/workflow/charge.go` (drop `activity.` qualifier)
- Modify: `internal/workflow/hook.go` (drop `activity.` qualifier)
- Modify: `internal/workflow/subscription_test.go` (drop `activityPkg.` qualifier)
- Modify: `cmd/worker/main.go` (drop activity import; use `workflow.` types)

After this task the `internal/activity` directory does not exist.

- [ ] **Step 1: Move the five files with git**

```bash
git mv internal/activity/payment.go internal/workflow/activity_payment.go
git mv internal/activity/billing.go internal/workflow/activity_billing.go
git mv internal/activity/hooks.go   internal/workflow/activity_hook.go
git mv internal/activity/retry.go   internal/workflow/retry.go
git mv internal/activity/errors.go  internal/workflow/errors.go
```

- [ ] **Step 2: Update package declarations**

In each of the five moved files, change the top line from `package activity` to `package workflow`. Use `sed` if you trust it, otherwise edit individually:

```bash
for f in internal/workflow/activity_payment.go internal/workflow/activity_billing.go internal/workflow/activity_hook.go internal/workflow/retry.go internal/workflow/errors.go; do
  sed -i '' '1s/^package activity$/package workflow/' "$f"
done
```

(macOS `sed -i ''`; Linux `sed -i`.)

Verify: `grep -l "^package activity" internal/workflow/*.go` should return nothing.

- [ ] **Step 3: Update `internal/workflow/charge.go`**

Remove the import `"github.com/martavoi/subflow/internal/activity"`. Replace every `activity.X` with `X` (same package now). The references are:

- `activity.ChargePaymentRetry` → `ChargePaymentRetry`
- `activity.ChargePayment{}` → `ChargePayment{}`
- `activity.ChargeResult` → `ChargeResult`
- `activity.BillingEventRetry` → `BillingEventRetry`

Use `grep -n "activity\." internal/workflow/charge.go` to enumerate.

- [ ] **Step 4: Update `internal/workflow/hook.go`**

Remove the import `"github.com/martavoi/subflow/internal/activity"`. Replace every `activity.X` with `X`:

- `activity.DispatchHook` → `DispatchHook`
- `activity.LifecycleData` → `LifecycleData`
- `activity.PaymentData` → `PaymentData`
- `activity.HookRetry` → `HookRetry`

- [ ] **Step 5: Update `internal/workflow/subscription_test.go`**

Remove the import alias line `activityPkg "github.com/martavoi/subflow/internal/activity"`. Replace every `activityPkg.X` with `X`:

- `activityPkg.ChargePayment` → `ChargePayment`
- `activityPkg.ChargeResult` → `ChargeResult`
- `activityPkg.DispatchHook` → `DispatchHook`

- [ ] **Step 6: Update `cmd/worker/main.go`**

Remove the line `"github.com/martavoi/subflow/internal/activity"` from imports. Replace the three handler-struct constructions to use `workflow` (already aliased as `wfpkg`):

```go
	paymentActs := &wfpkg.PaymentGateway{
		TransientFailureRate: cfg.PaymentTransientRate,
		TerminalFailureRate:  cfg.PaymentTerminalRate,
	}
	billingActs := &wfpkg.BillingStore{Events: billingStore}
	hookActs := &wfpkg.HookDispatcher{Client: intClient}
```

Activity registration names stay the same (`"ChargePayment"`, `"RecordBillingEvent"`, `"DispatchHook"`).

- [ ] **Step 7: Verify empty directory removed**

```bash
ls internal/activity 2>/dev/null || echo "removed"
```

Expected: `removed`. (`git mv` of the last file should have emptied the directory; if anything remains, investigate.)

- [ ] **Step 8: Verify build and tests**

Run: `go build ./... && go test ./...`
Expected: no errors, all tests pass.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(workflow): fold internal/activity into internal/workflow

Activity handlers, retry policies, and error type constants move into
package workflow with the activity_ filename prefix matching phase_.

See docs/adr/0006-fold-activity-into-workflow-package.md.
EOF
)"
```

---

## Task 4: Rename `Status` to `View`

**Files:**
- Rename with `git mv`: `internal/workflow/status.go` → `internal/workflow/view.go`
- Modify: `internal/workflow/view.go` (rename type)
- Modify: `internal/workflow/handlers.go` (rename handler method; update query registration)
- Modify: `internal/workflow/signals.go` (rename constants)
- Modify: `internal/server/subscriptions.go` (call sites)

- [ ] **Step 1: Rename the file**

```bash
git mv internal/workflow/status.go internal/workflow/view.go
```

- [ ] **Step 2: Rename the type and update its doc comment**

Edit `internal/workflow/view.go`. Final contents:

```go
package workflow

import "time"

// View is the read-model snapshot returned by the QuerySubscriptionView query.
// Distinct from Phase (lifecycle state); this carries period boundaries,
// counters, and the integrator-mutable Context for API rendering.
type View struct {
	UserID                string
	PlanCode              string
	Phase                 string
	PeriodStart           time.Time
	PeriodEnd             time.Time
	RenewalCount          int
	Context               Context
	CancelRequested       bool
	DunningAttempt        int
	LastChargedAt         time.Time
	LastChargeAmountCents int64
	LastFailureAt         time.Time
	LastFailureReason     string
	TotalChargedCents     int64
	SuccessfulChargeCount int
	FailedChargeCount     int
}
```

- [ ] **Step 3: Update `internal/workflow/signals.go`**

Replace the `QuerySubscriptionStatus` constant with `QuerySubscriptionView`. Final file:

```go
package workflow

// Signal, query, and update names exchanged with the SubscriptionWorkflow.
const (
	SignalCancelSubscription = "subscription.cancel"
	SignalUpdateContext      = "subscription.update_context"
	QuerySubscriptionView    = "subscription.view"
	UpdateActivate           = "subscription.activate"
)
```

- [ ] **Step 4: Update `internal/workflow/handlers.go`**

Rename `AsStatus` to `View`. Update its return type and the query registration. The relevant changes:

```go
// View is the query handler — returns the current entity snapshot for API
// rendering. Pure read; no mutation. Bound to s for use as a method-value
// handler.
func (s *Subscription) View() (View, error) {
	return View{
		UserID:                s.UserID,
		PlanCode:              s.PlanCode,
		Phase:                 string(s.Phase),
		PeriodStart:           s.Period.Start,
		PeriodEnd:             s.Period.End,
		RenewalCount:          s.RenewalCount,
		Context:               s.Context.Clone(),
		CancelRequested:       s.CancelRequested,
		DunningAttempt:        s.DunningAttempt,
		LastChargedAt:         s.LastChargedAt,
		LastChargeAmountCents: s.LastChargeAmountCents,
		LastFailureAt:         s.LastFailureAt,
		LastFailureReason:     s.LastFailureReason,
		TotalChargedCents:     s.TotalChargedCents,
		SuccessfulChargeCount: s.SuccessfulChargeCount,
		FailedChargeCount:     s.FailedChargeCount,
	}, nil
}
```

And in `registerHandlers`, update the query registration:

```go
	if err := workflow.SetQueryHandler(ctx, QuerySubscriptionView, s.View); err != nil {
		return err
	}
```

- [ ] **Step 5: Update `internal/server/subscriptions.go`**

There are three references to update:
- The query call: `workflow.QuerySubscriptionStatus` → `workflow.QuerySubscriptionView`
- The variable type: `var st workflow.Status` → `var st workflow.View`
- The helper function signature: `func statusToProto(id string, st workflow.Status) *subflowv1.Subscription` → `func viewToProto(id string, v workflow.View) *subflowv1.Subscription`. Rename the parameter to `v` inside the function body too (replace `st.` with `v.`). Update the one call site of `statusToProto(...)` to `viewToProto(...)`.

Use `grep -n "workflow.Status\|QuerySubscriptionStatus\|statusToProto" internal/server/subscriptions.go` first to enumerate exact lines.

- [ ] **Step 6: Verify build and tests**

Run: `go build ./... && go test ./...`
Expected: no errors, all tests pass.

Additional checks:
```bash
grep -rn "workflow\.Status\b\|QuerySubscriptionStatus\|AsStatus\|statusToProto" .
```
Expected: zero results.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(workflow): rename Status to View

workflow.View, subscription.view query, *Subscription.View() handler.
CONTEXT.md lists "Status" under _Avoid_ for Phase; the query returns a
read-model snapshot, not a status. See docs/adr/0004-rename-status-to-view.md.
EOF
)"
```

---

## Task 5: Introduce `hook.Event` and `hook.Payload`

**Files:**
- Create: `internal/hook/event.go`
- Test (optional but recommended): extend `internal/hook/hook_test.go` with a compile-time sealed-interface check.

This task adds the new types without using them. The workflow's `fireLifecycle`/`firePayment` continue to use the old `LifecycleData`/`PaymentData` defined in `activity_hook.go`. Task 6 swaps the usage.

- [ ] **Step 1: Create `internal/hook/event.go`**

```go
package hook

import "time"

// Event is the canonical integration-event payload published to integrators.
// Integration event (DDD-speak): published outward; the subscription workflow
// never reads its own emissions to drive internal state. See ADR 0002, 0007.
type Event struct {
	Type           Type
	SubscriptionID string
	UserID         string
	PlanCode       string
	RenewalCount   int
	OccurredAt     time.Time
	Context        map[string]string

	// Payload is the category-specific variant. Required; never nil for a
	// well-formed event.
	Payload Payload
}

// Payload is the category-specific variant carried on an Event. Sealed via
// the private isPayload marker — only payload types declared in this package
// satisfy it. New categories add a struct + isPayload().
type Payload interface{ isPayload() }

// LifecyclePayload carries the subscription's current phase and period
// boundaries — populated for hooks that report a state transition.
type LifecyclePayload struct {
	Phase       string
	PeriodStart time.Time
	PeriodEnd   time.Time
}

func (LifecyclePayload) isPayload() {}

// PaymentPayload carries charge-specific data — populated for hooks that
// report a payment outcome.
type PaymentPayload struct {
	DunningAttempt int
	AmountCents    int64
	Currency       string
	TransactionID  string
	FailureReason  string
}

func (PaymentPayload) isPayload() {}
```

- [ ] **Step 2: Add a compile-time check to `internal/hook/hook_test.go`**

Append to the end of `internal/hook/hook_test.go`:

```go
// Compile-time check that both payload types satisfy the sealed Payload
// interface. If a payload type stops satisfying it, this file won't compile.
var (
	_ hook.Payload = hook.LifecyclePayload{}
	_ hook.Payload = hook.PaymentPayload{}
)
```

- [ ] **Step 3: Verify build and tests**

Run: `go build ./... && go test ./...`
Expected: no errors, all tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/hook/event.go internal/hook/hook_test.go
git commit -m "$(cat <<'EOF'
feat(hook): introduce hook.Event with sealed Payload variant

LifecyclePayload and PaymentPayload satisfy the sealed Payload interface.
Used by Task 6 to replace workflow.LifecycleData/PaymentData.

See docs/adr/0007-integration-event-contract.md.
EOF
)"
```

---

## Task 6: Refactor workflow hook dispatch to use `hook.Event`

**Files:**
- Modify: `internal/workflow/activity_hook.go` (DispatchHook wraps hook.Event; type switch on payload; LifecycleData and PaymentData deleted)
- Modify: `internal/workflow/hook.go` (replace fireLifecycle / firePayment with emitLifecycle / emitPayment / dispatch / hookID)
- Modify: `internal/workflow/charge.go` (update payment emit call sites)
- Modify: `internal/workflow/phases.go` (update lifecycle emit call sites)
- Modify: `internal/workflow/subscription_test.go` (mock signature for DispatchHook input)

This is the largest task. Read the entire task before starting.

- [ ] **Step 1: Rewrite `internal/workflow/activity_hook.go`**

Replace the whole file with:

```go
package workflow

import (
	"context"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/hook"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DispatchHook is the activity input for delivering an integration event to
// the integrator. Wraps the canonical hook.Event with delivery metadata
// (Endpoint, EventID).
type DispatchHook struct {
	Event    hook.Event
	Endpoint string
	EventID  string // idempotency key
}

// HookDispatcher groups the hook dispatch activity. A single Dispatch method
// handles every payload variant via a type switch on Event.Payload.
type HookDispatcher struct {
	Client *integration.Client
}

// Dispatch is the single registered activity for all hook types. It builds
// a proto Event with the appropriate oneof payload variant and calls the
// integration's Dispatch rpc.
func (h *HookDispatcher) Dispatch(ctx context.Context, in DispatchHook) error {
	ev := &subflowv1.Event{
		Id:             in.EventID,
		Type:           string(in.Event.Type),
		CreatedAt:      timestamppb.New(in.Event.OccurredAt),
		Context:        in.Event.Context,
		SubscriptionId: in.Event.SubscriptionID,
		UserId:         in.Event.UserID,
		PlanCode:       in.Event.PlanCode,
		RenewalCount:   int32(in.Event.RenewalCount),
	}

	switch p := in.Event.Payload.(type) {
	case hook.LifecyclePayload:
		ev.Data = &subflowv1.Event_Lifecycle{
			Lifecycle: &subflowv1.LifecycleData{
				Phase:       p.Phase,
				PeriodStart: timestamppb.New(p.PeriodStart),
				PeriodEnd:   timestamppb.New(p.PeriodEnd),
			},
		}
	case hook.PaymentPayload:
		ev.Data = &subflowv1.Event_Payment{
			Payment: &subflowv1.PaymentData{
				DunningAttempt: int32(p.DunningAttempt),
				AmountCents:    p.AmountCents,
				Currency:       p.Currency,
				TransactionId:  p.TransactionID,
				FailureReason:  p.FailureReason,
			},
		}
	}

	return mapHookError(h.Client.Dispatch(ctx, in.Endpoint, ev))
}

// mapHookError converts gRPC errors to Temporal application errors. Terminal
// codes become non-retryable HookTerminalError; everything else stays
// retryable.
func mapHookError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.FailedPrecondition, codes.InvalidArgument, codes.NotFound, codes.Unimplemented:
			return temporal.NewNonRetryableApplicationError(st.Message(), ErrTypeHookTerminal, err)
		}
	}
	return temporal.NewApplicationError(err.Error(), "HookTransientError")
}
```

The old top-level `LifecycleData` and `PaymentData` types defined in this package are deleted (they now live in `internal/hook/event.go` as `LifecyclePayload` / `PaymentPayload`).

- [ ] **Step 2: Rewrite `internal/workflow/hook.go`**

Replace the whole file with:

```go
package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// isEnabled reports whether the plan opted into this hook.
func isEnabled(h hook.Type, enabled []hook.Type) bool {
	for _, e := range enabled {
		if e == h {
			return true
		}
	}
	return false
}

// emitLifecycle fires a lifecycle hook. Payload data is entirely derived
// from current subscription state — no extra arguments.
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

// emitPayment fires a payment hook. Caller supplies the full payload; no
// magic fill-in from subscription state.
func (s *Subscription) emitPayment(ctx workflow.Context, t hook.Type, p hook.PaymentPayload) error {
	return s.dispatch(ctx, hook.Event{Type: t, Payload: p})
}

// dispatch is the internal common-path for both emit variants: plan gating,
// common-field fill-in, idempotency key derivation, activity invocation.
func (s *Subscription) dispatch(ctx workflow.Context, e hook.Event) error {
	if s.Plan.IntegrationEndpoint == "" || !isEnabled(e.Type, s.Plan.EnabledHooks) {
		return nil
	}
	e.SubscriptionID = s.SubscriptionID
	e.UserID = s.UserID
	e.PlanCode = s.PlanCode
	e.RenewalCount = s.RenewalCount
	e.OccurredAt = workflow.Now(ctx)
	e.Context = map[string]string(s.Context)

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

// hookID derives the idempotency key. Payment payloads include DunningAttempt
// so retries on the same hook type across dunning attempts get distinct event
// IDs.
func (s *Subscription) hookID(e hook.Event) string {
	if p, ok := e.Payload.(hook.PaymentPayload); ok {
		return s.idempotencyKey(fmt.Sprintf("hook:%s:%d", e.Type, p.DunningAttempt))
	}
	return s.idempotencyKey("hook:" + string(e.Type))
}
```

- [ ] **Step 3: Update lifecycle emit call sites in `internal/workflow/phases.go`**

Replace `s.fireLifecycle(...)` with `s.emitLifecycle(...)` throughout. Use `grep -n "fireLifecycle" internal/workflow/phases.go` to enumerate the seven call sites (TrialStarted, TrialWillEnd, Activated, Renewed, PastDue, Recovered, Canceled, Deactivated, RenewalUpcoming).

Each line is a mechanical token swap; the argument list is unchanged:
- `s.fireLifecycle(ctx, hook.TrialStarted)` → `s.emitLifecycle(ctx, hook.TrialStarted)`
- ... and so on for each call.

- [ ] **Step 4: Update payment emit call sites in `internal/workflow/charge.go`**

The two `s.firePayment(...)` calls in `Charge()` become `s.emitPayment(...)`. Both call sites must construct a `hook.PaymentPayload` explicitly. The relevant block (success and failure paths near the end of `Charge`):

```go
	if chargeErr == nil {
		s.LastChargedAt = now
		s.LastChargeAmountCents = s.Plan.PriceCents
		s.TotalChargedCents += s.Plan.PriceCents
		s.SuccessfulChargeCount++
		_ = s.emitPayment(ctx, hook.PaymentOK, hook.PaymentPayload{
			DunningAttempt: dunningAttempt,
			AmountCents:    s.Plan.PriceCents,
			Currency:       s.Plan.Currency,
			TransactionID:  chargeRes.TransactionID,
		})
	} else {
		s.LastFailureAt = now
		s.LastFailureReason = chargeErr.Error()
		s.FailedChargeCount++
		_ = s.emitPayment(ctx, hook.PaymentFailed, hook.PaymentPayload{
			DunningAttempt: dunningAttempt,
			AmountCents:    s.Plan.PriceCents,
			Currency:       s.Plan.Currency,
			FailureReason:  chargeErr.Error(),
		})
	}
```

The `hook` import is already present in `charge.go`. No new import.

- [ ] **Step 5: Update `internal/workflow/subscription_test.go` mock**

The mock `mockDispatch` function (around line 84) is currently:

```go
mockDispatch := func(in DispatchHook) error {
    // ... assertions reference in.Type, in.Lifecycle, in.Payment, etc.
}
```

`DispatchHook` no longer has `Type`, `Lifecycle`, or `Payment` at top level — they live on `in.Event`. Update every field access:

- `in.Type` → `in.Event.Type`
- `in.Lifecycle` (nil-check) → `_, isLifecycle := in.Event.Payload.(hook.LifecyclePayload)`
- `in.Payment` (nil-check) → `_, isPayment := in.Event.Payload.(hook.PaymentPayload)`
- `in.SubscriptionID` → `in.Event.SubscriptionID`
- `in.UserID` → `in.Event.UserID`
- `in.PlanCode` → `in.Event.PlanCode`
- `in.RenewalCount` → `in.Event.RenewalCount`
- `in.EventTime` → `in.Event.OccurredAt`
- `in.Context` → `in.Event.Context`
- `in.EventID` → stays at `in.EventID` (delivery metadata, not part of `hook.Event`)
- `in.Endpoint` → stays at `in.Endpoint`

Read the file first with `Read internal/workflow/subscription_test.go` and walk the assertions inside `mockDispatch`. Update every field reference; do not change assertion logic.

- [ ] **Step 6: Verify build and tests**

Run: `go build ./... && go test ./...`
Expected: no errors, all tests pass.

Additional checks:
```bash
grep -rn "fireLifecycle\|firePayment\|LifecycleData\|PaymentData" internal/workflow
```
Expected: zero results.

```bash
grep -rn "LifecycleData\|PaymentData" internal/hook
```
Expected: zero results (the types are now `LifecyclePayload` / `PaymentPayload`).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(workflow): redesign hook dispatch around hook.Event

DispatchHook wraps the canonical hook.Event with delivery metadata.
Activity does a type switch on the sealed hook.Payload variant.
fireLifecycle/firePayment replaced with emitLifecycle/emitPayment
+ internal dispatch helper.

See docs/adr/0007-integration-event-contract.md.
EOF
)"
```

---

## Task 7: Split `phases.go` into one file per phase method

**Files:**
- Create: `internal/workflow/phase_trial.go`
- Create: `internal/workflow/phase_activation.go`
- Create: `internal/workflow/phase_renewal.go`
- Create: `internal/workflow/phase_dunning.go`
- Create: `internal/workflow/phase_await_end.go`
- Create: `internal/workflow/phase_deactivation.go`
- Create: `internal/workflow/phase_next_period.go`
- Delete: `internal/workflow/phases.go`

This is a mechanical split. Each phase method lives in its own file; helper types and helper functions stay with the methods they serve. No content changes beyond imports.

- [ ] **Step 1: Read `internal/workflow/phases.go`**

Before splitting, read the file fully so you know which method-helper pairs belong together. Use the Read tool.

- [ ] **Step 2: Create `internal/workflow/phase_trial.go`**

Move into it:
- `trialOutcome` int type + `trialOutcomeEnded` and `trialOutcomeCanceled` constants
- `(*Subscription).Trial(ctx workflow.Context) (trialOutcome, error)` method

Required imports: `"github.com/martavoi/subflow/internal/hook"`, `subflowtemporal "github.com/martavoi/subflow/internal/temporal"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 3: Create `internal/workflow/phase_activation.go`**

Move into it:
- `(*Subscription).AwaitActivation` method
- `(*Subscription).Activate` method

`ActivationResult` already lives in `subscription.go` — leave it there.

Required imports: `"github.com/martavoi/subflow/internal/hook"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 4: Create `internal/workflow/phase_renewal.go`**

Move into it:
- `(*Subscription).Renew` method

Required imports: `"github.com/martavoi/subflow/internal/hook"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 5: Create `internal/workflow/phase_dunning.go`**

Move into it:
- `ErrDunningExhausted` package-level variable
- `(*Subscription).Dun` method

Required imports: `"errors"`, `"time"`, `"github.com/martavoi/subflow/internal/hook"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 6: Create `internal/workflow/phase_await_end.go`**

Move into it:
- `(*Subscription).AwaitEnd` method
- `(*Subscription).cancelAndSleep` method

Required imports: `"github.com/martavoi/subflow/internal/hook"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 7: Create `internal/workflow/phase_deactivation.go`**

Move into it:
- `(*Subscription).Deactivate` method

Required imports: `"github.com/martavoi/subflow/internal/hook"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 8: Create `internal/workflow/phase_next_period.go`**

Move into it:
- `(*Subscription).NextPeriod` method
- `(*Subscription).toInput` method

Required imports: `"github.com/google/uuid"`, `subflowtemporal "github.com/martavoi/subflow/internal/temporal"`, `"go.temporal.io/sdk/workflow"`.

- [ ] **Step 9: Delete the old `phases.go`**

> **Do not run `go build` between steps 2 and 9.** Methods will be declared twice (in `phases.go` and in the new `phase_*.go`), so the build will fail with `(*Subscription).X redeclared`. Build only after `phases.go` is removed.

```bash
git rm internal/workflow/phases.go
```

- [ ] **Step 10: Verify build and tests**

Run: `go build ./... && go test ./...`
Expected: no errors, all tests pass.

If you accidentally duplicated a method declaration across two files, the compiler will say `(*Subscription).X redeclared in this block`. Find the duplicate and delete one copy.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(workflow): split phases.go into one file per phase method

phase_trial.go, phase_activation.go, phase_renewal.go, phase_dunning.go,
phase_await_end.go, phase_deactivation.go, phase_next_period.go.
Helpers stay with the methods they serve. No content changes.
EOF
)"
```

---

## Task 8: Update CONTEXT.md

**Files:**
- Modify: `CONTEXT.md`

Two additions: a `View` glossary entry (after the Subscription entry), and a one-line note under the Hook entry clarifying the DDD framing.

- [ ] **Step 1: Read `CONTEXT.md`**

Read the file to locate the Subscription glossary entry and the Hook glossary entry.

- [ ] **Step 2: Add the View glossary entry**

Insert this entry immediately after the Subscription entry (between Subscription and Plan) in the `## Language` section:

```markdown
**View**:
The read-model snapshot returned by the `subscription.view` query — current Phase, Billing Period, Context, and charge counters for API rendering. Distinct from Phase (lifecycle state); the View aggregates several observable facts.
_Avoid_: Status, State, Snapshot (Snapshot was a previous parallel name we removed; see ADR 0004 / 0005).
```

- [ ] **Step 3: Append the integration-event clarifier to the Hook entry**

Append a sentence to the body of the existing Hook entry:

> Hooks are **integration events** (DDD-speak): they are published outward; the workflow never consumes its own emissions to drive internal state. See ADR 0007.

- [ ] **Step 4: Verify**

Run: `go build ./... && go test ./...`
Expected: docs-only change, still green.

- [ ] **Step 5: Commit**

```bash
git add CONTEXT.md
git commit -m "$(cat <<'EOF'
docs: add View glossary entry; clarify Hook as integration event

View is the read-model returned by subscription.view; Hook events are
integration events, not domain events.
EOF
)"
```

---

## Task 9: Final verification

**Files:** none (pure verification).

Run the full sweep listed in the spec's Verification section before declaring complete.

- [ ] **Step 1: Build and test**

```bash
go build ./... && go test ./...
```
Expected: no errors, all tests pass.

- [ ] **Step 2: Confirm `internal/activity/` is gone**

```bash
test ! -d internal/activity && echo "ok"
```
Expected: `ok`.

- [ ] **Step 3: Confirm renames landed**

```bash
grep -rn "plan\.Snapshot\|SnapshotOf" .
```
Expected: zero results.

```bash
grep -rn "workflow\.Status\b\|QuerySubscriptionStatus\|AsStatus" .
```
Expected: zero results.

```bash
grep -rn "fireLifecycle\|firePayment" .
```
Expected: zero results.

```bash
grep -rn "activity\.LifecycleData\|activity\.PaymentData" .
```
Expected: zero results.

- [ ] **Step 4: Confirm phase split**

```bash
ls internal/workflow/phase_*.go | wc -l
```
Expected: `7`.

```bash
test ! -f internal/workflow/phases.go && echo "ok"
```
Expected: `ok`.

- [ ] **Step 5: Run the worker locally (smoke test)**

If a local Temporal/Mongo is configured, start the worker and confirm registration logs show `SubscriptionWorkflow`, `ChargePayment`, `RecordBillingEvent`, `DispatchHook`:

```bash
go run ./cmd/worker
```

(Stop with Ctrl-C after seeing `subflow-worker starting`.)

If no local stack is available, skip — the unit test suite covers handler registration paths.

- [ ] **Step 6: Final summary commit (optional)**

If any verification step turned up a fix, commit it. Otherwise the refactor is complete — no final commit needed.

---

## Reference: file layout after the refactor

```
internal/workflow/
  subscription.go          entity struct, NewSubscription, SubscriptionWorkflow, Run, transitionTo, idempotencyKey, ActivationResult
  input.go                 SubscriptionInput, Context, NextBillingPeriod
  signals.go               SignalCancelSubscription, SignalUpdateContext, QuerySubscriptionView, UpdateActivate
  handlers.go              registerHandlers, (*Subscription).View, OnCancel, OnContextUpdate
  view.go                  View struct
  charge.go                (*Subscription).Charge + chargePurpose
  hook.go                  emitLifecycle, emitPayment, dispatch, hookID, isEnabled

  phase_trial.go           Trial + trialOutcome enum
  phase_activation.go      AwaitActivation, Activate
  phase_renewal.go         Renew
  phase_dunning.go         Dun, ErrDunningExhausted
  phase_await_end.go       AwaitEnd, cancelAndSleep
  phase_deactivation.go    Deactivate
  phase_next_period.go     NextPeriod, toInput

  activity_payment.go      PaymentGateway, ChargePayment, ChargeResult
  activity_billing.go      BillingStore, RecordBillingEvent
  activity_hook.go         HookDispatcher, DispatchHook (wraps hook.Event)

  retry.go                 ChargePaymentRetry, BillingEventRetry, HookRetry
  errors.go                ErrType* constants

  subscription_test.go
  input_test.go

internal/hook/
  hook.go                  Type, All, Parse (unchanged)
  event.go                 Event, Payload (sealed), LifecyclePayload, PaymentPayload (new)
  hook_test.go             extended with sealed-interface compile check
```
