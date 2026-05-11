# subflow Temporal-Native Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Mongo `subscriptions_view` projection with Temporal Visibility, restructure the workflow as a struct-based `Subscription` entity, expand the lifecycle to 5 phases with 10 fire-and-forget hooks, and add an append-only `billing_events` collection.

**Architecture:** One workflow per subscription with the workflow as the sole source of subscription state. Custom search attributes (`SubflowUserId`, `SubflowPlanCode`, `SubflowPhase`, `SubflowPeriodEnd`, `SubflowTrialEnd`) drive listing via `client.ListWorkflow`. A new `internal/billing` package owns the append-only event history behind a swappable `EventStore` interface (Mongo today, Kafka later). The integration contract becomes a 10-method `SubscriptionHooks` gRPC service modeled on Stripe/Recurly/Chargebee/Apple/Google conventions — pure notifications, no flow control.

**Tech Stack:** Go 1.23+, `go.temporal.io/sdk` v1.43+, `go.mongodb.org/mongo-driver/v2`, `google.golang.org/grpc`, `buf` for proto codegen, `log/slog`.

**Spec:** `docs/superpowers/specs/2026-05-12-subflow-redesign.md`.

**Working branch:** `redesign/temporal-native` (already checked out off `main`).

---

## File structure

```
subflow/
├── api/v1/
│   ├── subflow.proto                # MODIFY: Plan + Subscription get new fields; new RPCs
│   ├── hooks.proto                  # NEW: SubscriptionHooks (10 methods)
│   ├── integration.proto            # DELETE
│   └── *.pb.go *_grpc.pb.go         # REGENERATED
├── internal/
│   ├── billing/                     # NEW
│   │   ├── event.go                 # Event + ListQuery
│   │   ├── store.go                 # EventStore interface
│   │   └── mongo_store.go           # MongoStore implementation
│   ├── temporal/                    # NEW
│   │   └── search_attributes.go     # SA constants + bootstrapper
│   ├── domain/
│   │   ├── plan/plan.go             # MODIFY: new fields
│   │   └── subscription/
│   │       ├── input.go             # MODIFY: PlanSnapshot, revised fields
│   │       ├── period.go            # KEEP (already pure)
│   │       └── context.go           # KEEP
│   ├── workflow/
│   │   ├── subscription.go          # REWRITE: entity struct + ctor + wrapper + Run + state-transition method
│   │   ├── trial.go                 # NEW: Trial method
│   │   ├── activation.go            # NEW: AwaitActivation, Activate, ActivationResult
│   │   ├── renewal.go               # NEW: Renew method
│   │   ├── dunning.go               # NEW: HandleDunning method
│   │   ├── cancellation.go          # NEW: AwaitPeriodEndOrCancellation, Deactivate
│   │   ├── continuation.go          # NEW: ContinueIntoNextPeriod
│   │   ├── handlers.go              # NEW: signal/query handlers + registerHandlers
│   │   ├── hook.go                  # NEW: Hook value object (10 typed constants) + FireLifecycleHook/FirePaymentHook methods on Subscription
│   │   ├── charge.go                # NEW: Charge method on Subscription (orchestrates payment + billing event + payment hook)
│   │   ├── signals.go               # MODIFY: signal/query/update NAME constants
│   │   ├── status.go                # NEW: Status type (query result)
│   │   ├── state.go                 # DELETE (replaced by entity struct)
│   │   ├── lifecycle.go             # DELETE (replaced by per-aspect files)
│   │   └── subscription_test.go     # REWRITE: 5 testsuite scenarios
│   ├── activity/
│   │   ├── errors.go                # MODIFY: HookTerminalError added
│   │   ├── retry.go                 # MODIFY: ChargePaymentRetry/BillingEventRetry/HookRetry
│   │   ├── payment.go               # KEEP (minor signature alignment)
│   │   ├── billing.go               # NEW: RecordBillingEvent
│   │   ├── hooks.go                 # NEW: 10 hook dispatch activities
│   │   ├── events.go                # DELETE
│   │   ├── integration.go           # DELETE
│   │   └── projection.go            # DELETE
│   ├── eventbus/                    # DELETE entire package
│   ├── integration/client.go        # REWRITE: SubscriptionHooks client
│   ├── server/
│   │   ├── plans.go                 # MODIFY: new fields + validation
│   │   ├── subscriptions.go         # MODIFY: Visibility listing + new endpoints
│   │   └── billing_events.go        # NEW: ListBillingEvents handler
│   ├── store/
│   │   ├── mongo.go                 # KEEP
│   │   ├── plans.go                 # MODIFY: new planDoc fields
│   │   └── projection.go            # DELETE
│   └── config/config.go             # KEEP (no changes needed)
├── cmd/
│   ├── api/main.go                  # MODIFY: SA bootstrapper, billing store wiring
│   ├── worker/main.go               # MODIFY: hook + billing activities, drop projection
│   └── mock-integration/main.go     # REWRITE: implements SubscriptionHooks
├── compose.yml                      # KEEP
├── Taskfile.yml                     # MODIFY: add list-billing-events + hook-related demos
├── README.md                        # MODIFY: explain new lifecycle + hooks + billing events
└── docs/
    ├── architecture.md              # MODIFY
    └── temporal-cheatsheet.md       # MODIFY: search attributes, dunning, hooks
```

**Tasks: 31 total.** Phase order: cleanup → proto → domain → storage → billing/temporal packages → activities → workflow entity → API → binaries → ops/docs → e2e.

---

## Task 1: Cleanup — delete files the redesign replaces

**Files:**
- Delete: `internal/eventbus/` (entire package)
- Delete: `internal/store/projection.go`
- Delete: `internal/activity/projection.go`
- Delete: `internal/activity/events.go`
- Delete: `internal/activity/integration.go`
- Delete: `internal/workflow/lifecycle.go`
- Delete: `internal/workflow/state.go`
- Delete: `internal/workflow/subscription.go`
- Delete: `internal/workflow/subscription_test.go`
- Delete: `api/v1/integration.proto`
- Delete: `api/v1/integration.pb.go`
- Delete: `api/v1/integration_grpc.pb.go`

The codebase will NOT build cleanly after this task. That's expected — subsequent tasks add the replacements. A `// SKIP` build comment will be added in any single file we keep but that depends on the deleted ones (e.g., `cmd/worker/main.go` references removed activities); that's part of T26 (worker rewrite). For now the goal is just removing the deprecated code.

- [ ] **Step 1: Delete the files**

```bash
cd /Users/dmartavoi/Projects/subflow
rm -rf internal/eventbus
rm -f internal/store/projection.go
rm -f internal/activity/projection.go
rm -f internal/activity/events.go
rm -f internal/activity/integration.go
rm -f internal/workflow/lifecycle.go
rm -f internal/workflow/state.go
rm -f internal/workflow/subscription.go
rm -f internal/workflow/subscription_test.go
rm -f api/v1/integration.proto
rm -f api/v1/integration.pb.go
rm -f api/v1/integration_grpc.pb.go
```

- [ ] **Step 2: Stage and commit**

```bash
cd /Users/dmartavoi/Projects/subflow
git add -A
git commit -m "chore: remove files replaced by Temporal-native redesign

Deleted: subscriptions_view projection (store + activity), eventbus
stub package, old integration.proto + generated code, old workflow
lifecycle/state files. Replacements come in subsequent commits."
```

- [ ] **Step 3: Verify (build will fail — expected)**

```bash
cd /Users/dmartavoi/Projects/subflow
go build ./... 2>&1 | head -20 || echo "Build failures expected at this stage."
git log --oneline -2
```

Expected: compile errors in `cmd/worker/main.go`, `cmd/api/main.go`, `internal/server/subscriptions.go`, `internal/workflow/signals.go` (referencing deleted symbols). These are addressed by later tasks. **Do not attempt to fix them now.**

---

## Task 2: Proto contracts — hooks.proto + subflow.proto

**Files:**
- Create: `api/v1/hooks.proto`
- Modify: `api/v1/subflow.proto`
- Regenerated: `api/v1/hooks.pb.go`, `api/v1/hooks_grpc.pb.go`, `api/v1/subflow.pb.go`, `api/v1/subflow_grpc.pb.go`

- [ ] **Step 1: Create `api/v1/hooks.proto`**

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

message HookAck {}
```

- [ ] **Step 2: Rewrite `api/v1/subflow.proto`**

Replace the entire file with:

```protobuf
syntax = "proto3";

package subflow.v1;

option go_package = "github.com/martavoi/subflow/api/v1;subflowv1";

import "google/protobuf/timestamp.proto";

service SubflowService {
  // Plans
  rpc CreatePlan(CreatePlanRequest)   returns (Plan);
  rpc GetPlan(GetPlanRequest)         returns (Plan);
  rpc ListPlans(ListPlansRequest)     returns (ListPlansResponse);
  rpc DeletePlan(DeletePlanRequest)   returns (DeletePlanResponse);

  // Subscriptions
  rpc CreateSubscription(CreateSubscriptionRequest)               returns (Subscription);
  rpc CancelSubscription(CancelSubscriptionRequest)               returns (CancelSubscriptionResponse);
  rpc GetSubscription(GetSubscriptionRequest)                     returns (Subscription);
  rpc ListSubscriptions(ListSubscriptionsRequest)                 returns (ListSubscriptionsResponse);
  rpc UpdateSubscriptionContext(UpdateSubscriptionContextRequest) returns (Subscription);

  // Billing history
  rpc ListBillingEvents(ListBillingEventsRequest)                 returns (ListBillingEventsResponse);
}

// ---------- Plan ----------

message Plan {
  string id = 1;
  string code = 2;
  string name = 3;
  string cadence = 4;                       // Go duration: "30s", "720h"
  int64 price_cents = 5;
  string currency = 6;                      // ISO 4217
  int32 per_user_limit = 7;                 // default 1; 0 = unlimited
  string trial_duration = 8;                // Go duration; empty = no trial
  string trial_end_notice_before = 9;       // Go duration; empty = no advance notice
  int32 dunning_max_attempts = 10;
  string dunning_retry_backoff = 11;        // Go duration; initial; doubles each attempt
  string integration_endpoint = 12;
  repeated string enabled_hooks = 13;
}

message CreatePlanRequest {
  string code = 1;
  string name = 2;
  string cadence = 3;
  int64 price_cents = 4;
  string currency = 5;
  int32 per_user_limit = 6;
  string trial_duration = 7;
  string trial_end_notice_before = 8;
  int32 dunning_max_attempts = 9;
  string dunning_retry_backoff = 10;
  string integration_endpoint = 11;
  repeated string enabled_hooks = 12;
}

message GetPlanRequest    { string id = 1; }
message ListPlansRequest  {}
message ListPlansResponse { repeated Plan plans = 1; }
message DeletePlanRequest { string id = 1; }
message DeletePlanResponse {}

// ---------- Subscription ----------

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

  // Billing summary
  int32 dunning_attempt = 10;
  google.protobuf.Timestamp last_charged_at = 11;
  int64 last_charge_amount_cents = 12;
  google.protobuf.Timestamp last_failure_at = 13;
  string last_failure_reason = 14;
  int64 total_charged_cents = 15;
  int32 successful_charge_count = 16;
  int32 failed_charge_count = 17;
}

message CreateSubscriptionRequest {
  string user_id = 1;
  string plan_id = 2;
  map<string,string> initial_context = 3;
}

message CancelSubscriptionRequest { string id = 1; }
message CancelSubscriptionResponse {}

message GetSubscriptionRequest { string id = 1; }

message ListSubscriptionsRequest {
  string user_id = 1;
  string plan_code = 2;
  string phase = 3;
  int32 page_size = 4;
  string page_token = 5;
}
message ListSubscriptionsResponse {
  repeated Subscription subscriptions = 1;
  string next_page_token = 2;
}

message UpdateSubscriptionContextRequest {
  string id = 1;
  map<string,string> updates = 2;
}

// ---------- Billing events ----------

message BillingEvent {
  string id = 1;
  string subscription_id = 2;
  string user_id = 3;
  string plan_code = 4;
  string type = 5;                          // "charged" | "failed" | "refunded"
  int64 amount_cents = 6;
  string currency = 7;
  google.protobuf.Timestamp period_start = 8;
  google.protobuf.Timestamp period_end = 9;
  int32 renewal_count = 10;
  int32 dunning_attempt = 11;
  string transaction_id = 12;
  string failure_reason = 13;
  google.protobuf.Timestamp occurred_at = 14;
}

message ListBillingEventsRequest {
  string subscription_id = 1;
  string user_id = 2;
  string type_filter = 3;
  int32 page_size = 4;
  string page_cursor = 5;
}
message ListBillingEventsResponse {
  repeated BillingEvent events = 1;
  string next_cursor = 2;
}
```

- [ ] **Step 3: Regenerate**

```bash
cd /Users/dmartavoi/Projects/subflow
buf generate
ls api/v1/*.pb.go
```

Expected: `hooks.pb.go`, `hooks_grpc.pb.go`, `subflow.pb.go`, `subflow_grpc.pb.go`. No `integration.*`.

- [ ] **Step 4: Verify generated code compiles**

```bash
go build ./api/v1/... 2>&1
```

Expected: clean build of the api package.

- [ ] **Step 5: Commit**

```bash
git add api/v1
git commit -m "feat(api): new hooks.proto + subflow.proto with redesign fields

- 10-method SubscriptionHooks service (fire-and-forget notifications)
- Plan adds: cadence, currency, per_user_limit, trial_*, dunning_*, enabled_hooks
- Subscription adds billing summary fields (last_charged_at, total_charged_cents, ...)
- New RPCs: UpdateSubscriptionContext, ListBillingEvents"
```

---

## Task 3: Domain types — Plan + SubscriptionInput + PlanSnapshot

**Files:**
- Modify: `internal/domain/plan/plan.go`
- Modify: `internal/domain/subscription/input.go`
- Modify: `internal/domain/subscription/period.go` (adjust `NextBillingPeriod`)
- Keep: `internal/domain/subscription/context.go`

- [ ] **Step 1: Rewrite `internal/domain/plan/plan.go`**

```go
package plan

import (
	"fmt"
	"time"
)

// Plan is a subscription plan aggregate. Persisted in the plans collection.
type Plan struct {
	ID                   string
	Code                 string
	Name                 string
	Cadence              time.Duration
	PriceCents           int64
	Currency             string
	PerUserLimit         int // 0 = unlimited
	TrialDuration        time.Duration
	TrialEndNoticeBefore time.Duration
	DunningMaxAttempts   int
	DunningRetryBackoff  time.Duration
	IntegrationEndpoint  string
	EnabledHooks         []HookName
	CreatedAt            time.Time
}

// HookName is the wire-name of a lifecycle hook. The workflow package
// converts HookName values into its typed Hook value object via HookByName.
// Stored verbatim in BSON (string-backed type round-trips cleanly).
type HookName string

const (
	HookNameTrialStarted  HookName = "subscription.trial_started"
	HookNameTrialWillEnd  HookName = "subscription.trial_will_end"
	HookNameActivated     HookName = "subscription.activated"
	HookNameRenewed       HookName = "subscription.renewed"
	HookNamePastDue       HookName = "subscription.past_due"
	HookNameRecovered     HookName = "subscription.recovered"
	HookNameCanceled      HookName = "subscription.canceled"
	HookNameDeactivated   HookName = "subscription.deactivated"
	HookNamePaymentOK     HookName = "payment.succeeded"
	HookNamePaymentFailed HookName = "payment.failed"
)

// AllHookNames is the canonical list — used for plan validation.
var AllHookNames = []HookName{
	HookNameTrialStarted, HookNameTrialWillEnd,
	HookNameActivated, HookNameRenewed,
	HookNamePastDue, HookNameRecovered,
	HookNameCanceled, HookNameDeactivated,
	HookNamePaymentOK, HookNamePaymentFailed,
}

// ParseHookNames converts a slice of raw strings (from the wire) into the
// typed HookName slice the plan persists. Rejects any unknown name. This is
// the only boundary parser in the package — domain validation belongs here,
// not in a server-side helper.
func ParseHookNames(raw []string) ([]HookName, error) {
	valid := make(map[string]HookName, len(AllHookNames))
	for _, h := range AllHookNames {
		valid[string(h)] = h
	}
	out := make([]HookName, 0, len(raw))
	for _, r := range raw {
		h, ok := valid[r]
		if !ok {
			return nil, fmt.Errorf("unknown hook name: %q (allowed: %v)", r, AllHookNames)
		}
		out = append(out, h)
	}
	return out, nil
}

// Snapshot is the immutable view of a plan that the workflow carries in its
// input. Workflow code reads only this — the workflow does not depend on
// Mongo or the live Plan record.
type Snapshot struct {
	Code                 string
	Cadence              time.Duration
	PriceCents           int64
	Currency             string
	TrialDuration        time.Duration
	TrialEndNoticeBefore time.Duration
	DunningMaxAttempts   int
	DunningRetryBackoff  time.Duration
	IntegrationEndpoint  string
	EnabledHooks         []HookName
}

// SnapshotOf builds a Snapshot from a full Plan.
func SnapshotOf(p Plan) Snapshot {
	return Snapshot{
		Code:                 p.Code,
		Cadence:              p.Cadence,
		PriceCents:           p.PriceCents,
		Currency:             p.Currency,
		TrialDuration:        p.TrialDuration,
		TrialEndNoticeBefore: p.TrialEndNoticeBefore,
		DunningMaxAttempts:   p.DunningMaxAttempts,
		DunningRetryBackoff:  p.DunningRetryBackoff,
		IntegrationEndpoint:  p.IntegrationEndpoint,
		EnabledHooks:         append([]HookName(nil), p.EnabledHooks...),
	}
}
```

- [ ] **Step 2: Rewrite `internal/domain/subscription/input.go`**

```go
package subscription

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
)

// SubscriptionInput is the workflow input. Carried across Continue-As-New
// so the next run can resume cleanly.
type SubscriptionInput struct {
	SubscriptionID  string
	UserID          string
	PlanID          string
	Plan            plan.Snapshot
	PeriodStart     time.Time
	PeriodEnd       time.Time
	Context         Context
	RenewalCount    int
	CancelRequested bool
}

// IsFirstPeriod reports whether this run represents the first billing period
// of the subscription (either trial or first paid).
func (in SubscriptionInput) IsFirstPeriod() bool {
	return in.RenewalCount == 0
}
```

- [ ] **Step 3: Update `internal/domain/subscription/period.go`**

The pure function now reads `Plan.Cadence` instead of an inline `BillingInterval` field:

```go
package subscription

// NextBillingPeriod returns a SubscriptionInput for the period immediately
// following `current`. Pure function (no time.Now, no randomness).
func NextBillingPeriod(current SubscriptionInput) SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  current.SubscriptionID,
		UserID:          current.UserID,
		PlanID:          current.PlanID,
		Plan:            current.Plan,
		PeriodStart:     current.PeriodEnd,
		PeriodEnd:       current.PeriodEnd.Add(current.Plan.Cadence),
		Context:         current.Context.Clone(),
		RenewalCount:    current.RenewalCount + 1,
		CancelRequested: false,
	}
}
```

- [ ] **Step 4: Update `internal/domain/subscription/period_test.go`**

Replace `BillingInterval` field references with `Plan.Cadence`:

```go
package subscription

import (
	"testing"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
)

func sample() SubscriptionInput {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return SubscriptionInput{
		SubscriptionID: "sub-1",
		UserID:         "user-1",
		PlanID:         "plan-1",
		Plan: plan.Snapshot{
			Code:    "monthly",
			Cadence: 30 * 24 * time.Hour,
		},
		PeriodStart:  start,
		PeriodEnd:    start.Add(30 * 24 * time.Hour),
		RenewalCount: 0,
		Context:      Context{"k": "v"},
	}
}

func TestNextBillingPeriod_AdvancesByCadence(t *testing.T) {
	in := sample()
	next := NextBillingPeriod(in)

	if !next.PeriodStart.Equal(in.PeriodEnd) {
		t.Fatalf("PeriodStart = %v, want %v", next.PeriodStart, in.PeriodEnd)
	}
	if !next.PeriodEnd.Equal(in.PeriodEnd.Add(in.Plan.Cadence)) {
		t.Fatalf("PeriodEnd = %v, want %v", next.PeriodEnd, in.PeriodEnd.Add(in.Plan.Cadence))
	}
	if next.RenewalCount != in.RenewalCount+1 {
		t.Fatalf("RenewalCount = %d, want %d", next.RenewalCount, in.RenewalCount+1)
	}
	if next.CancelRequested {
		t.Fatalf("CancelRequested should reset to false")
	}
}

func TestNextBillingPeriod_PreservesIdentityFields(t *testing.T) {
	in := sample()
	in.Plan.PriceCents = 1234
	in.Plan.IntegrationEndpoint = "mock:50052"
	next := NextBillingPeriod(in)

	if next.UserID != in.UserID || next.PlanID != in.PlanID {
		t.Fatalf("identity lost")
	}
	if next.Plan.Code != in.Plan.Code || next.Plan.PriceCents != in.Plan.PriceCents {
		t.Fatalf("plan snapshot lost")
	}
	if next.Plan.IntegrationEndpoint != in.Plan.IntegrationEndpoint {
		t.Fatalf("integration endpoint lost")
	}
}

func TestNextBillingPeriod_ContextIsCloned(t *testing.T) {
	in := sample()
	next := NextBillingPeriod(in)
	next.Context["k"] = "mutated"
	if in.Context["k"] != "v" {
		t.Fatalf("mutating next leaked back into input")
	}
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/domain/... -v
```

Expected: 3 tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/domain
git commit -m "feat(domain): plan adds trial/dunning/per-user-limit/hooks; SubscriptionInput carries plan.Snapshot"
```

---

## Task 4: Storage — plans repository for new Plan fields

**Files:**
- Modify: `internal/store/plans.go`
- Keep: `internal/store/mongo.go`

- [ ] **Step 1: Rewrite `internal/store/plans.go`**

```go
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrPlanNotFound = errors.New("plan not found")

type planDoc struct {
	ID                   string          `bson:"_id"`
	Code                 string          `bson:"code"`
	Name                 string          `bson:"name"`
	Cadence              string          `bson:"cadence"` // Go duration string
	PriceCents           int64           `bson:"price_cents"`
	Currency             string          `bson:"currency"`
	PerUserLimit         int             `bson:"per_user_limit"`
	TrialDuration        string          `bson:"trial_duration"`
	TrialEndNoticeBefore string          `bson:"trial_end_notice_before"`
	DunningMaxAttempts   int             `bson:"dunning_max_attempts"`
	DunningRetryBackoff  string          `bson:"dunning_retry_backoff"`
	IntegrationEndpoint  string          `bson:"integration_endpoint"`
	EnabledHooks         []plan.HookName `bson:"enabled_hooks"` // string-backed; encodes as []string
	CreatedAt            time.Time       `bson:"created_at"`
}

type PlanRepository struct {
	col *mongo.Collection
}

func NewPlanRepository(db *mongo.Database) *PlanRepository {
	return &PlanRepository{col: db.Collection("plans")}
}

func (r *PlanRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "code", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("plans_code_unique"),
	})
	return err
}

func (r *PlanRepository) Insert(ctx context.Context, p plan.Plan) error {
	_, err := r.col.InsertOne(ctx, planToDoc(p))
	return err
}

func (r *PlanRepository) Get(ctx context.Context, id string) (plan.Plan, error) {
	var d planDoc
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return plan.Plan{}, ErrPlanNotFound
	}
	if err != nil {
		return plan.Plan{}, err
	}
	return docToPlan(d)
}

func (r *PlanRepository) GetByCode(ctx context.Context, code string) (plan.Plan, error) {
	var d planDoc
	err := r.col.FindOne(ctx, bson.M{"code": code}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return plan.Plan{}, ErrPlanNotFound
	}
	if err != nil {
		return plan.Plan{}, err
	}
	return docToPlan(d)
}

func (r *PlanRepository) List(ctx context.Context) ([]plan.Plan, error) {
	cur, err := r.col.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]plan.Plan, 0)
	for cur.Next(ctx) {
		var d planDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		p, err := docToPlan(d)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, cur.Err()
}

func (r *PlanRepository) Delete(ctx context.Context, id string) error {
	res, err := r.col.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return ErrPlanNotFound
	}
	return nil
}

func planToDoc(p plan.Plan) planDoc {
	return planDoc{
		ID:                   p.ID,
		Code:                 p.Code,
		Name:                 p.Name,
		Cadence:              p.Cadence.String(),
		PriceCents:           p.PriceCents,
		Currency:             p.Currency,
		PerUserLimit:         p.PerUserLimit,
		TrialDuration:        durationString(p.TrialDuration),
		TrialEndNoticeBefore: durationString(p.TrialEndNoticeBefore),
		DunningMaxAttempts:   p.DunningMaxAttempts,
		DunningRetryBackoff:  durationString(p.DunningRetryBackoff),
		IntegrationEndpoint:  p.IntegrationEndpoint,
		EnabledHooks:         append([]plan.HookName(nil), p.EnabledHooks...),
		CreatedAt:            p.CreatedAt,
	}
}

func docToPlan(d planDoc) (plan.Plan, error) {
	cadence, err := time.ParseDuration(d.Cadence)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse cadence %q: %w", d.Cadence, err)
	}
	trial, err := parseOptional(d.TrialDuration)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse trial_duration: %w", err)
	}
	notice, err := parseOptional(d.TrialEndNoticeBefore)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse trial_end_notice_before: %w", err)
	}
	backoff, err := parseOptional(d.DunningRetryBackoff)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse dunning_retry_backoff: %w", err)
	}
	return plan.Plan{
		ID:                   d.ID,
		Code:                 d.Code,
		Name:                 d.Name,
		Cadence:              cadence,
		PriceCents:           d.PriceCents,
		Currency:             d.Currency,
		PerUserLimit:         d.PerUserLimit,
		TrialDuration:        trial,
		TrialEndNoticeBefore: notice,
		DunningMaxAttempts:   d.DunningMaxAttempts,
		DunningRetryBackoff:  backoff,
		IntegrationEndpoint:  d.IntegrationEndpoint,
		EnabledHooks:         append([]string(nil), d.EnabledHooks...),
		CreatedAt:            d.CreatedAt,
	}, nil
}

func durationString(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}

func parseOptional(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/store/...
```

Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add internal/store/plans.go
git commit -m "feat(store): plan repository carries trial/dunning/per-user-limit/hooks fields"
```

---

## Task 5: Billing package — Event, EventStore, MongoStore

**Files:**
- Create: `internal/billing/event.go`
- Create: `internal/billing/store.go`
- Create: `internal/billing/mongo_store.go`

- [ ] **Step 1: Create `internal/billing/event.go`**

```go
package billing

import "time"

// Type values for Event.Type.
const (
	TypeCharged  = "charged"
	TypeFailed   = "failed"
	TypeRefunded = "refunded" // reserved for roadmap
)

// Event is an immutable billing record. One Event = one charge attempt (success
// or failure) on a subscription. Append-only — never updated, never deleted.
type Event struct {
	ID             string    // primary key; deterministic from workflow context
	SubscriptionID string
	UserID         string
	PlanCode       string
	Type           string
	AmountCents    int64
	Currency       string
	PeriodStart    time.Time
	PeriodEnd      time.Time
	RenewalCount   int
	DunningAttempt int
	TransactionID  string // empty on failure
	FailureReason  string // empty on success
	OccurredAt     time.Time
	Reference      string // idempotency token (workflowID:runID:purpose)
}

// ListQuery filters and paginates Event lookups.
type ListQuery struct {
	SubscriptionID string
	UserID         string
	TypeFilter     string
	PageSize       int    // default 50, capped at 200
	PageCursor     string // opaque, occurred_at-based
}

// Page caps and defaults applied by stores.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)
```

- [ ] **Step 2: Create `internal/billing/store.go`**

```go
package billing

import "context"

// EventStore is the interface every billing event backend implements.
// MongoStore is the current concrete; a future KafkaStore can swap in without
// changing the workflow or activity layer.
type EventStore interface {
	// Append writes an Event. Implementations must be idempotent on Event.Reference
	// (a retry with the same reference returns nil without inserting a duplicate).
	Append(ctx context.Context, ev Event) error

	// List returns events matching the query, newest first, and a cursor for
	// the next page (empty when no more pages).
	List(ctx context.Context, q ListQuery) (events []Event, nextCursor string, err error)
}
```

- [ ] **Step 3: Create `internal/billing/mongo_store.go`**

```go
package billing

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStore struct {
	col *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{col: db.Collection("billing_events")}
}

// EnsureIndexes creates the unique index on reference and the compound indexes
// used by List. Idempotent — re-running is safe.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "reference", Value: 1}}, Options: options.Index().SetUnique(true).SetName("reference_unique")},
		{Keys: bson.D{{Key: "subscription_id", Value: 1}, {Key: "occurred_at", Value: -1}}, Options: options.Index().SetName("sub_occurred")},
		{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "occurred_at", Value: -1}}, Options: options.Index().SetName("user_occurred")},
	})
	return err
}

type eventDoc struct {
	ID             string    `bson:"_id"`
	SubscriptionID string    `bson:"subscription_id"`
	UserID         string    `bson:"user_id"`
	PlanCode       string    `bson:"plan_code"`
	Type           string    `bson:"type"`
	AmountCents    int64     `bson:"amount_cents"`
	Currency       string    `bson:"currency"`
	PeriodStart    time.Time `bson:"period_start"`
	PeriodEnd      time.Time `bson:"period_end"`
	RenewalCount   int       `bson:"renewal_count"`
	DunningAttempt int       `bson:"dunning_attempt"`
	TransactionID  string    `bson:"transaction_id"`
	FailureReason  string    `bson:"failure_reason"`
	OccurredAt     time.Time `bson:"occurred_at"`
	Reference      string    `bson:"reference"`
}

func (s *MongoStore) Append(ctx context.Context, ev Event) error {
	doc := eventDoc{
		ID:             ev.ID,
		SubscriptionID: ev.SubscriptionID,
		UserID:         ev.UserID,
		PlanCode:       ev.PlanCode,
		Type:           ev.Type,
		AmountCents:    ev.AmountCents,
		Currency:       ev.Currency,
		PeriodStart:    ev.PeriodStart,
		PeriodEnd:      ev.PeriodEnd,
		RenewalCount:   ev.RenewalCount,
		DunningAttempt: ev.DunningAttempt,
		TransactionID:  ev.TransactionID,
		FailureReason:  ev.FailureReason,
		OccurredAt:     ev.OccurredAt,
		Reference:      ev.Reference,
	}
	_, err := s.col.InsertOne(ctx, doc)
	if err != nil && mongo.IsDuplicateKeyError(err) {
		return nil // idempotent
	}
	return err
}

func (s *MongoStore) List(ctx context.Context, q ListQuery) ([]Event, string, error) {
	size := q.PageSize
	if size <= 0 {
		size = DefaultPageSize
	}
	if size > MaxPageSize {
		size = MaxPageSize
	}

	filter := bson.M{}
	if q.SubscriptionID != "" {
		filter["subscription_id"] = q.SubscriptionID
	}
	if q.UserID != "" {
		filter["user_id"] = q.UserID
	}
	if q.TypeFilter != "" {
		filter["type"] = q.TypeFilter
	}
	if q.PageCursor != "" {
		cursorTime, err := decodeCursor(q.PageCursor)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page_cursor: %w", err)
		}
		filter["occurred_at"] = bson.M{"$lt": cursorTime}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "occurred_at", Value: -1}}).
		SetLimit(int64(size + 1)) // fetch one extra to know if there's a next page

	cur, err := s.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", err
	}
	defer cur.Close(ctx)

	out := make([]Event, 0, size)
	for cur.Next(ctx) {
		var d eventDoc
		if err := cur.Decode(&d); err != nil {
			return nil, "", err
		}
		out = append(out, docToEvent(d))
	}
	if err := cur.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(out) > size {
		nextCursor = encodeCursor(out[size-1].OccurredAt)
		out = out[:size]
	}
	return out, nextCursor, nil
}

func docToEvent(d eventDoc) Event {
	return Event{
		ID:             d.ID,
		SubscriptionID: d.SubscriptionID,
		UserID:         d.UserID,
		PlanCode:       d.PlanCode,
		Type:           d.Type,
		AmountCents:    d.AmountCents,
		Currency:       d.Currency,
		PeriodStart:    d.PeriodStart,
		PeriodEnd:      d.PeriodEnd,
		RenewalCount:   d.RenewalCount,
		DunningAttempt: d.DunningAttempt,
		TransactionID:  d.TransactionID,
		FailureReason:  d.FailureReason,
		OccurredAt:     d.OccurredAt,
		Reference:      d.Reference,
	}
}

func encodeCursor(t time.Time) string {
	return base64.URLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano)))
}

func decodeCursor(c string) (time.Time, error) {
	raw, err := base64.URLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, string(raw))
	if err != nil {
		return time.Time{}, err
	}
	if t.IsZero() {
		return time.Time{}, errors.New("zero cursor time")
	}
	return t, nil
}
```

- [ ] **Step 4: Verify it compiles**

```bash
go build ./internal/billing/...
```

- [ ] **Step 5: Commit**

```bash
git add internal/billing
git commit -m "feat(billing): EventStore interface + Mongo implementation with idempotent append + cursor pagination"
```

---

## Task 6: Temporal package — search attributes + bootstrapper

**Files:**
- Create: `internal/temporal/search_attributes.go`

- [ ] **Step 1: Create `internal/temporal/search_attributes.go`**

```go
package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
)

// Custom search attribute names used by SubscriptionWorkflow.
const (
	AttrUserId    = "SubflowUserId"
	AttrPlanCode  = "SubflowPlanCode"
	AttrPhase     = "SubflowPhase"
	AttrPeriodEnd = "SubflowPeriodEnd"
	AttrTrialEnd  = "SubflowTrialEnd"
)

// attrTypes is the canonical (name, type) registry. EnsureSearchAttributes
// registers each name with the given type if it doesn't already exist.
var attrTypes = []struct {
	Name string
	Type enumspb.IndexedValueType
}{
	{AttrUserId, enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	{AttrPlanCode, enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	{AttrPhase, enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	{AttrPeriodEnd, enumspb.INDEXED_VALUE_TYPE_DATETIME},
	{AttrTrialEnd, enumspb.INDEXED_VALUE_TYPE_DATETIME},
}

// EnsureSearchAttributes registers the custom subflow search attributes on
// the Temporal cluster. Already-existing attributes are treated as success.
// Run this once at API startup; safe to re-run.
func EnsureSearchAttributes(ctx context.Context, c client.Client, namespace string, logger *slog.Logger) error {
	op := c.OperatorService()
	for _, a := range attrTypes {
		_, err := op.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
			Namespace: namespace,
			SearchAttributes: map[string]enumspb.IndexedValueType{
				a.Name: a.Type,
			},
		})
		if err == nil {
			logger.Info("registered search attribute", slog.String("name", a.Name))
			continue
		}
		if isAlreadyExistsError(err) {
			logger.Debug("search attribute already exists", slog.String("name", a.Name))
			continue
		}
		return fmt.Errorf("register %q: %w", a.Name, err)
	}
	return nil
}

// isAlreadyExistsError detects the Temporal "already exists" status without
// importing the grpc/status package. The error message contains "already exists"
// across all Temporal versions that support custom SAs.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return errors.Is(err, errors.New("AlreadyExists")) ||
		stringContainsFold(msg, "already exists") ||
		stringContainsFold(msg, "AlreadyExists")
}

func stringContainsFold(s, sub string) bool {
	return len(s) >= len(sub) && indexOfFold(s, sub) >= 0
}

func indexOfFold(s, sub string) int {
	if sub == "" {
		return 0
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/temporal/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/temporal
git commit -m "feat(temporal): search attribute constants + EnsureSearchAttributes bootstrapper"
```

---

## Task 7: Activity errors + retry policies

**Files:**
- Modify: `internal/activity/errors.go`
- Modify: `internal/activity/retry.go`

- [ ] **Step 1: Rewrite `internal/activity/errors.go`**

```go
package activity

// Application error types used by activities. Names appear in RetryPolicy
// NonRetryableErrorTypes lists, matched against ApplicationError.Type exactly.
const (
	// Payment terminal failures — never retry.
	ErrTypeInsufficientFunds = "InsufficientFundsError"
	ErrTypeCardDeclined      = "CardDeclinedError"

	// Payment transient — retried by Temporal per ChargePaymentRetry.
	ErrTypePaymentGatewayTimeout = "PaymentGatewayTimeoutError"

	// Integration hook terminal — never retry; integrator explicitly rejected.
	ErrTypeHookTerminal = "HookTerminalError"

	// Billing event store gave up — workflow logs and continues.
	ErrTypeBillingStoreExhausted = "BillingStoreExhaustedError"
)
```

- [ ] **Step 2: Rewrite `internal/activity/retry.go`**

```go
package activity

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// ChargePaymentRetry handles transient payment-gateway failures. Stops on
// terminal billing errors (declined, insufficient funds).
var ChargePaymentRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    5,
	NonRetryableErrorTypes: []string{
		ErrTypeInsufficientFunds,
		ErrTypeCardDeclined,
	},
}

// BillingEventRetry is bounded — if Mongo is down for 20 minutes, we give up
// at the activity boundary. The charge already happened; the workflow logs a
// critical error and proceeds. Workflow history is the forensic fallback.
var BillingEventRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    20,
}

// HookRetry handles transient integration failures (gRPC Unavailable, etc.).
// Unlimited retries — hooks must eventually deliver. Integrators that want
// to permanently fail return HookTerminalError.
var HookRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    5 * time.Minute,
	MaximumAttempts:    0,
	NonRetryableErrorTypes: []string{
		ErrTypeHookTerminal,
	},
}
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/activity/...
```

Expected: clean — `errors.go` + `retry.go` + the existing `payment.go` (which references these constants).

- [ ] **Step 4: Commit**

```bash
git add internal/activity/errors.go internal/activity/retry.go
git commit -m "feat(activity): retry policies for charge/billing-event/hook; HookTerminal error type"
```

---

## Task 8: ChargePayment activity (signature alignment with Plan.Currency)

**Files:**
- Modify: `internal/activity/payment.go`

- [ ] **Step 1: Rewrite `internal/activity/payment.go`**

```go
package activity

import (
	"context"
	"log/slog"
	"math/rand/v2"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type ChargePaymentInput struct {
	Reference   string
	UserID      string
	PlanCode    string
	AmountCents int64
	Currency    string
}

type ChargePaymentResult struct {
	Reference     string
	TransactionID string
	AmountCents   int64
	Currency      string
}

// PaymentActivities is the mocked payment gateway. In production this would
// hold a real Stripe/Adyen/etc. client; here we inject failures probabilistically.
type PaymentActivities struct {
	TransientFailureRate float64
	TerminalFailureRate  float64
}

// ChargePayment simulates a charge attempt. Returns terminal or transient
// errors based on configured rates. Idempotent on Reference at the (mocked)
// gateway level — a retry with the same reference returns the cached result.
func (a *PaymentActivities) ChargePayment(ctx context.Context, in ChargePaymentInput) (ChargePaymentResult, error) {
	logger := activity.GetLogger(ctx)

	r := rand.Float64()
	switch {
	case r < a.TerminalFailureRate:
		logger.Warn("ChargePayment terminal (declined)", slog.String("ref", in.Reference))
		return ChargePaymentResult{}, temporal.NewNonRetryableApplicationError(
			"card declined", ErrTypeCardDeclined, nil)
	case r < a.TerminalFailureRate+a.TransientFailureRate:
		logger.Warn("ChargePayment transient gateway timeout", slog.String("ref", in.Reference))
		return ChargePaymentResult{}, temporal.NewApplicationError(
			"payment gateway timeout", ErrTypePaymentGatewayTimeout)
	}

	logger.Info("ChargePayment success",
		slog.String("ref", in.Reference),
		slog.String("user", in.UserID),
		slog.String("plan", in.PlanCode),
		slog.Int64("cents", in.AmountCents),
		slog.String("currency", in.Currency))

	return ChargePaymentResult{
		Reference:     in.Reference,
		TransactionID: "txn-" + in.Reference,
		AmountCents:   in.AmountCents,
		Currency:      in.Currency,
	}, nil
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/activity/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/activity/payment.go
git commit -m "feat(activity): ChargePayment input/result carry PlanCode + Currency"
```

---

## Task 9: Billing activity — RecordBillingEvent

**Files:**
- Create: `internal/activity/billing.go`

- [ ] **Step 1: Create `internal/activity/billing.go`**

```go
package activity

import (
	"context"

	"github.com/martavoi/subflow/internal/billing"
)

// BillingActivities holds the EventStore dependency for billing-related activities.
type BillingActivities struct {
	Events billing.EventStore
}

// RecordBillingEvent appends a single billing event. Implementations of
// EventStore are required to be idempotent on Event.Reference, so this activity
// is safe to retry without producing duplicates.
func (a *BillingActivities) RecordBillingEvent(ctx context.Context, ev billing.Event) error {
	return a.Events.Append(ctx, ev)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/activity/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/activity/billing.go
git commit -m "feat(activity): RecordBillingEvent dispatching to EventStore"
```

---

## Task 10: Integration client — SubscriptionHooks stubs

**Files:**
- Rewrite: `internal/integration/client.go`

- [ ] **Step 1: Rewrite `internal/integration/client.go`**

```go
package integration

import (
	"context"
	"fmt"
	"sync"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client maintains lazy-dialed gRPC connections to integration endpoints
// (one connection per host). The SubscriptionHooks stub is exposed via Hooks.
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewClient() *Client {
	return &Client{conns: make(map[string]*grpc.ClientConn)}
}

// Hooks returns a SubscriptionHooks gRPC client for the given endpoint,
// reusing the existing connection or dialing a new one.
func (c *Client) Hooks(endpoint string) (subflowv1.SubscriptionHooksClient, error) {
	conn, err := c.connect(endpoint)
	if err != nil {
		return nil, err
	}
	return subflowv1.NewSubscriptionHooksClient(conn), nil
}

func (c *Client) connect(endpoint string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[endpoint]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %q: %w", endpoint, err)
	}
	c.conns[endpoint] = conn
	return conn, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// DispatchLifecycle calls one of the lifecycle hook methods on the integration
// service. Centralizes the dispatch so the activity layer doesn't need a switch.
func (c *Client) DispatchLifecycle(ctx context.Context, endpoint, hookName string, ev *subflowv1.LifecycleEvent) error {
	stub, err := c.Hooks(endpoint)
	if err != nil {
		return err
	}
	switch hookName {
	case "subscription.trial_started":
		_, err = stub.OnTrialStarted(ctx, ev)
	case "subscription.trial_will_end":
		_, err = stub.OnTrialWillEnd(ctx, ev)
	case "subscription.activated":
		_, err = stub.OnActivated(ctx, ev)
	case "subscription.renewed":
		_, err = stub.OnRenewed(ctx, ev)
	case "subscription.past_due":
		_, err = stub.OnPastDue(ctx, ev)
	case "subscription.recovered":
		_, err = stub.OnRecovered(ctx, ev)
	case "subscription.canceled":
		_, err = stub.OnCanceled(ctx, ev)
	case "subscription.deactivated":
		_, err = stub.OnDeactivated(ctx, ev)
	default:
		return fmt.Errorf("unknown lifecycle hook: %s", hookName)
	}
	return err
}

// DispatchPayment calls one of the payment hook methods.
func (c *Client) DispatchPayment(ctx context.Context, endpoint, hookName string, ev *subflowv1.PaymentEvent) error {
	stub, err := c.Hooks(endpoint)
	if err != nil {
		return err
	}
	switch hookName {
	case "payment.succeeded":
		_, err = stub.OnPaymentSucceeded(ctx, ev)
	case "payment.failed":
		_, err = stub.OnPaymentFailed(ctx, ev)
	default:
		return fmt.Errorf("unknown payment hook: %s", hookName)
	}
	return err
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/integration/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/integration/client.go
git commit -m "feat(integration): Client.DispatchLifecycle + DispatchPayment for SubscriptionHooks"
```

---

## Task 11: Hook activities — one activity per hook name

**Files:**
- Create: `internal/activity/hooks.go`

- [ ] **Step 1: Create `internal/activity/hooks.go`**

```go
package activity

import (
	"context"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// LifecycleHookInput is the activity input for the 8 subscription-level hooks.
// The workflow assembles this struct from its current state and passes it to
// whichever hook activity the plan opted into.
type LifecycleHookInput struct {
	Reference           string
	IntegrationEndpoint string
	HookName            string
	SubscriptionID      string
	UserID              string
	PlanCode            string
	Phase               string
	RenewalCount        int
	PeriodStart         time.Time
	PeriodEnd           time.Time
	EventTime           time.Time
	Context             map[string]string
}

// PaymentHookInput is the activity input for the 2 payment-level hooks.
type PaymentHookInput struct {
	Reference           string
	IntegrationEndpoint string
	HookName            string
	SubscriptionID      string
	UserID              string
	PlanCode            string
	RenewalCount        int
	DunningAttempt      int
	AmountCents         int64
	Currency            string
	TransactionID       string
	FailureReason       string
	EventTime           time.Time
	Context             map[string]string
}

// HookActivities groups the 10 hook dispatch activities, all of which share
// the same integration client.
type HookActivities struct {
	Client *integration.Client
}

// dispatchLifecycle is the shared code path for all 8 lifecycle hook activities.
// Each registered method (OnTrialStarted, OnActivated, etc.) is a thin wrapper.
func (a *HookActivities) dispatchLifecycle(ctx context.Context, in LifecycleHookInput) error {
	ev := &subflowv1.LifecycleEvent{
		Reference:      in.Reference,
		SubscriptionId: in.SubscriptionID,
		UserId:         in.UserID,
		PlanCode:       in.PlanCode,
		Phase:          in.Phase,
		RenewalCount:   int32(in.RenewalCount),
		PeriodStart:    timestamppb.New(in.PeriodStart),
		PeriodEnd:      timestamppb.New(in.PeriodEnd),
		EventTime:      timestamppb.New(in.EventTime),
		Context:        in.Context,
	}
	err := a.Client.DispatchLifecycle(ctx, in.IntegrationEndpoint, in.HookName, ev)
	return mapHookError(err)
}

func (a *HookActivities) dispatchPayment(ctx context.Context, in PaymentHookInput) error {
	ev := &subflowv1.PaymentEvent{
		Reference:      in.Reference,
		SubscriptionId: in.SubscriptionID,
		UserId:         in.UserID,
		PlanCode:       in.PlanCode,
		RenewalCount:   int32(in.RenewalCount),
		DunningAttempt: int32(in.DunningAttempt),
		AmountCents:    in.AmountCents,
		Currency:       in.Currency,
		TransactionId:  in.TransactionID,
		FailureReason:  in.FailureReason,
		EventTime:      timestamppb.New(in.EventTime),
		Context:        in.Context,
	}
	err := a.Client.DispatchPayment(ctx, in.IntegrationEndpoint, in.HookName, ev)
	return mapHookError(err)
}

// mapHookError converts gRPC errors to Temporal application errors. Terminal
// codes (FailedPrecondition / InvalidArgument / NotFound / Unimplemented) become
// non-retryable HookTerminalError; everything else stays retryable.
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

// ---- 10 registered methods. Each just forwards to dispatch* — the registered
// activity name is what the workflow calls; the methods all share the same
// body. We keep them as discrete registered names so the Temporal Web UI
// shows the hook name on every activity execution.

func (a *HookActivities) OnTrialStarted(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnTrialWillEnd(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnActivated(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnRenewed(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnPastDue(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnRecovered(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnCanceled(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnDeactivated(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}
func (a *HookActivities) OnPaymentSucceeded(ctx context.Context, in PaymentHookInput) error {
	return a.dispatchPayment(ctx, in)
}
func (a *HookActivities) OnPaymentFailed(ctx context.Context, in PaymentHookInput) error {
	return a.dispatchPayment(ctx, in)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/activity/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/activity/hooks.go
git commit -m "feat(activity): 10 hook activities dispatching to SubscriptionHooks gRPC stub"
```

---

## Task 12: Workflow scaffolding — entity struct + state-transition method + signals + status

**Files:**
- Create: `internal/workflow/signals.go`
- Create: `internal/workflow/status.go`
- Create: `internal/workflow/subscription.go`

The entity owns its state-machine transitions and its idempotency token. Neither is a "helper" — they're domain methods on `Subscription`. The file `subscription.go` is the entity body; per-phase lifecycle methods live in their own files (T13–T20).

- [ ] **Step 1: Create `internal/workflow/signals.go`**

```go
package workflow

// Signal, query, and update names exchanged with the SubscriptionWorkflow.
// These strings are part of the public API — clients reference them directly.
const (
	SignalCancelSubscription = "subscription.cancel"
	SignalUpdateContext      = "subscription.update_context"
	QuerySubscriptionStatus  = "subscription.status"
	UpdateActivate           = "subscription.activate"
)
```

- [ ] **Step 2: Create `internal/workflow/status.go`**

```go
package workflow

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
)

// Status is the snapshot returned by the QuerySubscriptionStatus query.
// Mirrors the live entity state visible to API callers.
type Status struct {
	Phase                 string
	PeriodStart           time.Time
	PeriodEnd             time.Time
	RenewalCount          int
	Context               subscription.Context
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

- [ ] **Step 3: Create `internal/workflow/subscription.go`**

This is the entity struct + constructor + the top-level wrapper function that the worker registers. The `Run` method body is filled in later (Task 21); for now it's a stub that returns nil so other files can reference `Subscription` methods.

```go
package workflow

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// Phase enumerates the lifecycle states of a Subscription.
type Phase string

const (
	PhaseStarting    Phase = "starting"
	PhaseTrialing    Phase = "trialing"
	PhaseActive      Phase = "active"
	PhasePastDue     Phase = "past_due"
	PhaseCanceled    Phase = "canceled"
	PhaseDeactivated Phase = "deactivated"
)

// BillingPeriod is the half-open interval [Start, End) the subscription is
// currently billing for.
type BillingPeriod struct {
	Start time.Time
	End   time.Time
}

// Subscription is the entity-workflow aggregate root. Every domain verb is a
// method. All state lives in fields. No I/O — activities do that.
type Subscription struct {
	// Identity (immutable)
	ID       string
	UserID   string
	PlanID   string
	PlanCode string

	// Plan snapshot — immutable for this run
	Plan plan.Snapshot

	// Lifecycle state
	Phase           Phase
	Period          BillingPeriod
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool

	// Dunning state
	DunningAttempt int

	// Billing summary (O(1), updated on each charge attempt)
	LastChargedAt         time.Time
	LastChargeAmountCents int64
	LastFailureAt         time.Time
	LastFailureReason     string
	TotalChargedCents     int64
	SuccessfulChargeCount int
	FailedChargeCount     int
}

// NewSubscription builds a fresh Subscription entity from workflow input. Pure;
// safe to call during workflow replay.
func NewSubscription(in subscription.SubscriptionInput) *Subscription {
	ctxCopy := in.Context.Clone()
	return &Subscription{
		ID:              in.SubscriptionID,
		UserID:          in.UserID,
		PlanID:          in.PlanID,
		PlanCode:        in.Plan.Code,
		Plan:            in.Plan,
		Phase:           PhaseStarting,
		Period:          BillingPeriod{Start: in.PeriodStart, End: in.PeriodEnd},
		RenewalCount:    in.RenewalCount,
		Context:         ctxCopy,
		CancelRequested: in.CancelRequested,
	}
}

// SubscriptionWorkflow is the top-level workflow function registered with the
// worker. It constructs a Subscription entity and runs it. The real lifecycle
// logic lives in (*Subscription).Run (filled in by Task 21).
func SubscriptionWorkflow(ctx workflow.Context, in subscription.SubscriptionInput) error {
	return NewSubscription(in).Run(ctx)
}

// Run is the workflow body. Stub for now; populated by Task 21 once all
// lifecycle methods exist.
func (s *Subscription) Run(ctx workflow.Context) error {
	// Real body added in Task 21. Returning nil here so the package builds
	// while Tasks 13-20 add the per-aspect method files.
	_ = ctx
	return nil
}

// ActivationResult is the response to the UpdateActivate update — returned
// synchronously to the API caller via UpdateWithStartWorkflow.
type ActivationResult struct {
	Phase   string
	Context subscription.Context
}

// transitionTo moves the entity into a new lifecycle phase AND publishes the
// change via the SubflowPhase search attribute so Visibility queries reflect
// it. Co-locating the two operations enforces the invariant: every phase
// change is observable from outside the workflow.
func (s *Subscription) transitionTo(ctx workflow.Context, p Phase) {
	s.Phase = p
	_ = workflow.UpsertSearchAttributes(ctx, map[string]any{
		subflowtemporal.AttrPhase: string(p),
	})
}

// idempotencyKey builds the activity idempotency token used wherever the
// workflow calls an activity that has at-least-once semantics. Stable across
// retries within a run; unique across runs (run ID changes each CAN).
func (s *Subscription) idempotencyKey(ctx workflow.Context, purpose string) string {
	info := workflow.GetInfo(ctx)
	return fmt.Sprintf("%s:%s:%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, purpose)
}
```

Note: this file imports `fmt` and `github.com/martavoi/subflow/internal/temporal as subflowtemporal`. Update the import block accordingly:

```go
import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)
```

- [ ] **Step 4: Verify it compiles**

```bash
go build ./internal/workflow/...
```

Expected: clean build (the workflow package now has the entity struct + types but no method logic yet).

- [ ] **Step 5: Commit**

```bash
git add internal/workflow/signals.go internal/workflow/status.go internal/workflow/subscription.go
git commit -m "feat(workflow): entity Subscription + Phase + ActivationResult + workflow registration wrapper"
```

---

## Task 13: Charge — the domain operation that turns money into a billing event

**Files:**
- Create: `internal/workflow/charge.go`

`Charge` is a first-class domain verb on the Subscription entity: it orchestrates the charge attempt, records the resulting billing event, updates the summary fields on the entity, and fires the matching payment hook. It is not a "helper" — it's the central monetary operation that every period (activation, renewal, dunning retry) routes through.

- [ ] **Step 1: Create `internal/workflow/charge.go`**

```go
package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/billing"
	"go.temporal.io/sdk/workflow"
)

// chargePurpose is a closed enum for the charge call sites. Replaces the
// stringly-typed purpose argument the previous design carried around.
type chargePurpose string

const (
	chargeActivation chargePurpose = "activation"
	chargeRenewal    chargePurpose = "renewal"
	chargeDunning    chargePurpose = "dunning"
)

// Charge attempts a single payment, records the resulting billing event,
// updates entity summary fields, and fires the matching payment hook.
// Returns the original charge error so callers (Activate, Renew,
// HandleDunning) can route the workflow accordingly.
func (s *Subscription) Charge(ctx workflow.Context, purpose chargePurpose, dunningAttempt int) error {
	ref := s.idempotencyKey(ctx, fmt.Sprintf("charge:%s:%d", purpose, dunningAttempt))

	chargeOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.ChargePaymentRetry,
	})
	chargeIn := activity.ChargePaymentInput{
		Reference:   ref,
		UserID:      s.UserID,
		PlanCode:    s.PlanCode,
		AmountCents: s.Plan.PriceCents,
		Currency:    s.Plan.Currency,
	}
	var chargeRes activity.ChargePaymentResult
	chargeErr := workflow.ExecuteActivity(chargeOpts, "ChargePayment", chargeIn).Get(ctx, &chargeRes)

	s.recordBillingEvent(ctx, ref, dunningAttempt, chargeRes, chargeErr)
	s.applyChargeOutcome(ctx, chargeRes, chargeErr, dunningAttempt)

	return chargeErr
}

// recordBillingEvent writes an append-only billing record for this charge
// attempt. Idempotent at the EventStore layer; we don't propagate the result
// because the charge already happened — workflow history is the forensic
// fallback if the billing store is permanently down.
func (s *Subscription) recordBillingEvent(ctx workflow.Context, ref string, dunningAttempt int, res activity.ChargePaymentResult, chargeErr error) {
	now := workflow.Now(ctx)
	eventType := billing.TypeCharged
	failureReason := ""
	if chargeErr != nil {
		eventType = billing.TypeFailed
		failureReason = chargeErr.Error()
	}
	ev := billing.Event{
		ID:             ref,
		SubscriptionID: s.ID,
		UserID:         s.UserID,
		PlanCode:       s.PlanCode,
		Type:           eventType,
		AmountCents:    s.Plan.PriceCents,
		Currency:       s.Plan.Currency,
		PeriodStart:    s.Period.Start,
		PeriodEnd:      s.Period.End,
		RenewalCount:   s.RenewalCount,
		DunningAttempt: dunningAttempt,
		TransactionID:  res.TransactionID,
		FailureReason:  failureReason,
		OccurredAt:     now,
		Reference:      ref,
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         activity.BillingEventRetry,
	})
	_ = workflow.ExecuteActivity(opts, "RecordBillingEvent", ev).Get(ctx, nil)
}

// applyChargeOutcome updates the entity's O(1) summary fields and fires the
// payment hook. Pure entity mutation + one hook dispatch.
func (s *Subscription) applyChargeOutcome(ctx workflow.Context, res activity.ChargePaymentResult, chargeErr error, dunningAttempt int) {
	now := workflow.Now(ctx)
	if chargeErr == nil {
		s.LastChargedAt = now
		s.LastChargeAmountCents = s.Plan.PriceCents
		s.TotalChargedCents += s.Plan.PriceCents
		s.SuccessfulChargeCount++
		_ = s.FirePaymentHook(ctx, HookPaymentOK, dunningAttempt, res.TransactionID, "")
		return
	}
	s.LastFailureAt = now
	s.LastFailureReason = chargeErr.Error()
	s.FailedChargeCount++
	_ = s.FirePaymentHook(ctx, HookPaymentFailed, dunningAttempt, "", chargeErr.Error())
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/workflow/...
```

Expected: compile FAILS — `FirePaymentHook`, `HookPaymentOK`, `HookPaymentFailed` are defined in T14. Stage the file:

```bash
git add internal/workflow/charge.go
```

(Don't commit yet — wait for T14.)

---

## Task 14: Hook — typed value object + dispatch methods on Subscription

**Files:**
- Create: `internal/workflow/hook.go`

Replaces the stringly-typed `hookActivityName(string) string` switch with a `Hook` value object that co-locates both the wire name (`subscription.activated`) and the registered activity name (`OnActivated`). The "is this hook enabled?" check becomes a method on `Hook` reading the plan's typed `EnabledHooks []Hook` slice. No switch, no helpers, no stringly-typed dispatch.

- [ ] **Step 1: Create `internal/workflow/hook.go`**

```go
package workflow

import (
	"slices"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"go.temporal.io/sdk/workflow"
)

// Hook is a typed identifier for a lifecycle event the integration can
// subscribe to. Each Hook bundles its wire name (the string used over the
// network and in plan configuration) with the registered Temporal activity
// name. Co-locating both eliminates name-mapping switches.
type Hook struct {
	Name         string // wire name, e.g. "subscription.activated"
	ActivityName string // registered Temporal activity name, e.g. "OnActivated"
}

// String makes Hook printable via the wire name.
func (h Hook) String() string { return h.Name }

// The 10 hooks subflow supports. Defined as package-level variables so they
// can be compared by value and referenced from plans, tests, and the proto
// gateway.
var (
	HookTrialStarted  = Hook{Name: "subscription.trial_started", ActivityName: "OnTrialStarted"}
	HookTrialWillEnd  = Hook{Name: "subscription.trial_will_end", ActivityName: "OnTrialWillEnd"}
	HookActivated     = Hook{Name: "subscription.activated", ActivityName: "OnActivated"}
	HookRenewed       = Hook{Name: "subscription.renewed", ActivityName: "OnRenewed"}
	HookPastDue       = Hook{Name: "subscription.past_due", ActivityName: "OnPastDue"}
	HookRecovered     = Hook{Name: "subscription.recovered", ActivityName: "OnRecovered"}
	HookCanceled      = Hook{Name: "subscription.canceled", ActivityName: "OnCanceled"}
	HookDeactivated   = Hook{Name: "subscription.deactivated", ActivityName: "OnDeactivated"}
	HookPaymentOK     = Hook{Name: "payment.succeeded", ActivityName: "OnPaymentSucceeded"}
	HookPaymentFailed = Hook{Name: "payment.failed", ActivityName: "OnPaymentFailed"}
)

// AllHooks is the canonical list — used for plan validation, tests, and
// activity registration.
var AllHooks = []Hook{
	HookTrialStarted, HookTrialWillEnd,
	HookActivated, HookRenewed,
	HookPastDue, HookRecovered,
	HookCanceled, HookDeactivated,
	HookPaymentOK, HookPaymentFailed,
}

// HookByName returns the Hook for a wire name, or (Hook{}, false) if unknown.
// The plan/server layer uses this when parsing client requests.
func HookByName(name string) (Hook, bool) {
	for _, h := range AllHooks {
		if h.Name == name {
			return h, true
		}
	}
	return Hook{}, false
}

// isEnabled reports whether the plan opted into this hook. The plan carries
// its enabled set as a typed []Hook (see plan.Snapshot.EnabledHooks); this
// is a slice membership check on values.
func (h Hook) isEnabled(plan planSnapshotHookView) bool {
	return slices.Contains(plan.EnabledHooks, h)
}

// planSnapshotHookView is a structural interface on plan.Snapshot — declared
// here to avoid a domain → workflow import. plan.Snapshot.EnabledHooks must
// have type []Hook (T3 sets this up).
type planSnapshotHookView interface{ HookEnabled(Hook) bool }
```

- [ ] **Step 2: Update `internal/domain/plan/plan.go` — type EnabledHooks as `[]Hook`**

This requires `plan.Snapshot` to expose a typed slice. Because `plan` can't import `workflow` (would cycle), we add the typed slice via a tiny adapter type in the plan package:

```go
// In internal/domain/plan/plan.go, change EnabledHooks fields:

type Plan struct {
	// ... other fields ...
	EnabledHooks []HookName  // typed wire-name slice
	// ...
}

type Snapshot struct {
	// ... other fields ...
	EnabledHooks []HookName
	// ...
}

// HookName is the wire-name of a hook. The workflow package converts these
// to typed Hook value objects via workflow.HookByName.
type HookName string

const (
	HookNameTrialStarted  HookName = "subscription.trial_started"
	HookNameTrialWillEnd  HookName = "subscription.trial_will_end"
	HookNameActivated     HookName = "subscription.activated"
	HookNameRenewed       HookName = "subscription.renewed"
	HookNamePastDue       HookName = "subscription.past_due"
	HookNameRecovered     HookName = "subscription.recovered"
	HookNameCanceled      HookName = "subscription.canceled"
	HookNameDeactivated   HookName = "subscription.deactivated"
	HookNamePaymentOK     HookName = "payment.succeeded"
	HookNamePaymentFailed HookName = "payment.failed"
)

var ValidHookNames = []HookName{
	HookNameTrialStarted, HookNameTrialWillEnd,
	HookNameActivated, HookNameRenewed,
	HookNamePastDue, HookNameRecovered,
	HookNameCanceled, HookNameDeactivated,
	HookNamePaymentOK, HookNamePaymentFailed,
}
```

Wherever Task 3 had `EnabledHooks []string`, change to `EnabledHooks []HookName`. Wherever Task 4 stored `enabled_hooks` as BSON `[]string`, decode into `[]HookName` (string-backed type — round-trips cleanly with BSON's string codec). Wherever Task 23 validated `req.EnabledHooks` against `ValidHookNames`, the iteration becomes type-safe.

- [ ] **Step 3: Update `internal/workflow/hook.go` to use the plan domain type directly**

Now that `plan.Snapshot.EnabledHooks` is typed, `Hook.isEnabled` reads it directly:

```go
// In internal/workflow/hook.go, replace planSnapshotHookView + isEnabled with:

// isEnabled reports whether the plan opted into this hook.
func (h Hook) isEnabled(enabled []plan.HookName) bool {
	for _, n := range enabled {
		if string(n) == h.Name {
			return true
		}
	}
	return false
}
```

(Add `import "github.com/martavoi/subflow/internal/domain/plan"` to the file. No interface adapter needed; we just consume the domain type directly.)

- [ ] **Step 4: Add `FireLifecycleHook` and `FirePaymentHook` as methods on Subscription**

Still in `internal/workflow/hook.go`:

```go
// FireLifecycleHook dispatches a subscription-level hook. No-op if the plan
// has no integration endpoint or didn't opt into this hook.
func (s *Subscription) FireLifecycleHook(ctx workflow.Context, h Hook) error {
	if s.Plan.IntegrationEndpoint == "" || !h.isEnabled(s.Plan.EnabledHooks) {
		return nil
	}
	ref := s.idempotencyKey(ctx, "hook:"+h.Name)
	in := activity.LifecycleHookInput{
		Reference:           ref,
		IntegrationEndpoint: s.Plan.IntegrationEndpoint,
		HookName:            h.Name,
		SubscriptionID:      s.ID,
		UserID:              s.UserID,
		PlanCode:            s.PlanCode,
		Phase:               string(s.Phase),
		RenewalCount:        s.RenewalCount,
		PeriodStart:         s.Period.Start,
		PeriodEnd:           s.Period.End,
		EventTime:           workflow.Now(ctx),
		Context:             map[string]string(s.Context),
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.HookRetry,
	})
	return workflow.ExecuteActivity(opts, h.ActivityName, in).Get(ctx, nil)
}

// FirePaymentHook dispatches a payment-level hook. Same gating + retry policy
// as FireLifecycleHook; different payload.
func (s *Subscription) FirePaymentHook(ctx workflow.Context, h Hook, dunningAttempt int, transactionID, failureReason string) error {
	if s.Plan.IntegrationEndpoint == "" || !h.isEnabled(s.Plan.EnabledHooks) {
		return nil
	}
	ref := s.idempotencyKey(ctx, "hook:"+h.Name)
	in := activity.PaymentHookInput{
		Reference:           ref,
		IntegrationEndpoint: s.Plan.IntegrationEndpoint,
		HookName:            h.Name,
		SubscriptionID:      s.ID,
		UserID:              s.UserID,
		PlanCode:            s.PlanCode,
		RenewalCount:        s.RenewalCount,
		DunningAttempt:      dunningAttempt,
		AmountCents:         s.Plan.PriceCents,
		Currency:            s.Plan.Currency,
		TransactionID:       transactionID,
		FailureReason:       failureReason,
		EventTime:           workflow.Now(ctx),
		Context:             map[string]string(s.Context),
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.HookRetry,
	})
	return workflow.ExecuteActivity(opts, h.ActivityName, in).Get(ctx, nil)
}
```

- [ ] **Step 5: Update callers in Tasks 16–20**

Wherever Tasks 16–20 had `s.fireLifecycleHook(ctx, "subscription.activated")` — replace with `s.FireLifecycleHook(ctx, HookActivated)`. Same substitution for all 8 lifecycle hooks. Tasks 16–20 in this plan still show the old strings; when implementing, the engineer substitutes per the table:

| Old (string) | New (typed) |
|---|---|
| `s.fireLifecycleHook(ctx, "subscription.trial_started")` | `s.FireLifecycleHook(ctx, HookTrialStarted)` |
| `s.fireLifecycleHook(ctx, "subscription.trial_will_end")` | `s.FireLifecycleHook(ctx, HookTrialWillEnd)` |
| `s.fireLifecycleHook(ctx, "subscription.activated")` | `s.FireLifecycleHook(ctx, HookActivated)` |
| `s.fireLifecycleHook(ctx, "subscription.renewed")` | `s.FireLifecycleHook(ctx, HookRenewed)` |
| `s.fireLifecycleHook(ctx, "subscription.past_due")` | `s.FireLifecycleHook(ctx, HookPastDue)` |
| `s.fireLifecycleHook(ctx, "subscription.recovered")` | `s.FireLifecycleHook(ctx, HookRecovered)` |
| `s.fireLifecycleHook(ctx, "subscription.canceled")` | `s.FireLifecycleHook(ctx, HookCanceled)` |
| `s.fireLifecycleHook(ctx, "subscription.deactivated")` | `s.FireLifecycleHook(ctx, HookDeactivated)` |

`charge.go` (T13) already uses `HookPaymentOK` / `HookPaymentFailed` — no further substitution needed.

- [ ] **Step 6: Verify it compiles**

```bash
go build ./internal/workflow/...
```

Expected: clean build (charge.go's references to `HookPaymentOK`, `HookPaymentFailed`, `FirePaymentHook` now resolve).

- [ ] **Step 7: Commit charge + hooks together (they form one coherent unit)**

```bash
git add internal/workflow/charge.go internal/workflow/hook.go internal/domain/plan/plan.go
git commit -m "feat(workflow): typed Hook value object + Charge as a first-class domain operation

Replaces helpers.go with two domain-shaped files:
- charge.go: Charge / recordBillingEvent / applyChargeOutcome as methods on Subscription
- hook.go: typed Hook value object pairing wire name with activity name;
  FireLifecycleHook + FirePaymentHook as methods on Subscription;
  plan.HookName replaces []string for EnabledHooks."
```

---

## Task 15: Workflow handlers — signal/query/update registration

**Files:**
- Create: `internal/workflow/handlers.go`

- [ ] **Step 1: Create `internal/workflow/handlers.go`**

```go
package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// AsStatus is the query handler — returns the current entity snapshot.
// Pure read; no mutation. Bound to s for use as a method-value handler.
func (s *Subscription) AsStatus() (Status, error) {
	return Status{
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

// HandleCancelSignal flips the cancel flag. The main loop reads it on the
// next selector wakeup. Phase doesn't transition here — that happens in
// AwaitPeriodEndOrCancellation (or Trial).
func (s *Subscription) HandleCancelSignal(_ workflow.Context, _ struct{}) {
	s.CancelRequested = true
}

// HandleContextUpdateSignal merges integrator-supplied context updates into
// the subscription's mutable bag.
func (s *Subscription) HandleContextUpdateSignal(_ workflow.Context, updates map[string]string) {
	if s.Context == nil {
		s.Context = subscription.Context{}
	}
	for k, v := range updates {
		s.Context[k] = v
	}
}

// registerHandlers wires up query + signal handlers. The Activate UPDATE
// handler is registered separately inside AwaitActivation because it's only
// valid on the first paid period.
func (s *Subscription) registerHandlers(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, QuerySubscriptionStatus, s.AsStatus); err != nil {
		return err
	}
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
	return nil
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/workflow/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/handlers.go
git commit -m "feat(workflow): AsStatus query + cancel/context signal handlers + registerHandlers"
```

---

## Task 16: Workflow trial phase

**Files:**
- Create: `internal/workflow/trial.go`

- [ ] **Step 1: Create `internal/workflow/trial.go`**

```go
package workflow

import (
	"time"

	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)

// trialOutcome reports how the trial phase ended.
type trialOutcome int

const (
	trialOutcomeEnded    trialOutcome = iota // trial period elapsed naturally
	trialOutcomeCanceled                     // cancel signal received during trial
)

// Trial parks the workflow in trialing phase until either the trial period
// elapses or a cancel signal arrives. Fires subscription.trial_started on
// entry; fires subscription.trial_will_end at TrialEndNoticeBefore (if set).
func (s *Subscription) Trial(ctx workflow.Context) (trialOutcome, error) {
	s.transitionTo(ctx, PhaseTrialing)

	// Set the trial-end SA for "trial expiring" queries.
	_ = workflow.UpsertSearchAttributes(ctx, map[string]any{
		subflowtemporal.AttrTrialEnd: s.Period.End,
	})

	_ = s.fireLifecycleHook(ctx, "subscription.trial_started")

	now := workflow.Now(ctx)
	trialEnd := s.Period.End

	// Optional advance-notice timer.
	if s.Plan.TrialEndNoticeBefore > 0 {
		noticeAt := trialEnd.Add(-s.Plan.TrialEndNoticeBefore)
		if noticeAt.After(now) {
			// Wait for either the notice timer, trial end, or cancel.
			outcome, fired := s.awaitTrialPhase(ctx, noticeAt, trialEnd, /*alreadyNoticed*/ false)
			if outcome != trialOutcomeNoticeFired {
				return outcome, nil
			}
			_ = fired
			_ = s.fireLifecycleHook(ctx, "subscription.trial_will_end")
		}
	}

	// Wait for trial end or cancel (notice already fired above, if at all).
	outcome, _ := s.awaitTrialPhase(ctx, time.Time{}, trialEnd, /*alreadyNoticed*/ true)
	return outcome, nil
}

// awaitTrialPhase returns one of three internal outcomes:
//   - trialOutcomeNoticeFired: the notice timer fired first (caller fires the hook and re-enters).
//   - trialOutcomeEnded:        the trial-end timer fired.
//   - trialOutcomeCanceled:     a cancel signal was observed (via s.CancelRequested).
//
// noticeAt is zero if no notice is wanted in this call (re-entry case).
const trialOutcomeNoticeFired trialOutcome = 100

func (s *Subscription) awaitTrialPhase(ctx workflow.Context, noticeAt, trialEnd time.Time, alreadyNoticed bool) (trialOutcome, bool) {
	now := workflow.Now(ctx)
	endTimer := workflow.NewTimer(ctx, trialEnd.Sub(now))

	sel := workflow.NewSelector(ctx)
	noticeFired := false
	ended := false

	if !alreadyNoticed && !noticeAt.IsZero() && noticeAt.After(now) {
		noticeTimer := workflow.NewTimer(ctx, noticeAt.Sub(now))
		sel.AddFuture(noticeTimer, func(workflow.Future) { noticeFired = true })
	}
	sel.AddFuture(endTimer, func(workflow.Future) { ended = true })

	for !noticeFired && !ended && !s.CancelRequested {
		sel.Select(ctx)
	}

	switch {
	case s.CancelRequested:
		return trialOutcomeCanceled, false
	case noticeFired:
		return trialOutcomeNoticeFired, true
	default:
		return trialOutcomeEnded, true
	}
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/workflow/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/trial.go
git commit -m "feat(workflow): Trial method with optional trial_will_end notice + cancel handling"
```

---

## Task 17: Workflow activation phase

**Files:**
- Create: `internal/workflow/activation.go`

- [ ] **Step 1: Create `internal/workflow/activation.go`**

```go
package workflow

import (
	"go.temporal.io/sdk/workflow"
)

// AwaitActivation registers the Activate update handler and blocks on
// workflow.Await until activation completes. The handler runs Activate(),
// which performs the first-period charge + hooks. Returns whatever
// Activate returned (nil on success, error on charge failure).
//
// The Activate update is sent by the API immediately after starting the
// workflow via client.UpdateWithStartWorkflow.
func (s *Subscription) AwaitActivation(ctx workflow.Context) error {
	activated := false
	var activationErr error

	if err := workflow.SetUpdateHandler(ctx, UpdateActivate,
		func(ctx workflow.Context) (ActivationResult, error) {
			if err := s.Activate(ctx); err != nil {
				activationErr = err
				activated = true // unblock the Await; Run will deactivate
				return ActivationResult{}, err
			}
			activated = true
			return ActivationResult{
				Phase:   string(s.Phase),
				Context: s.Context.Clone(),
			}, nil
		},
	); err != nil {
		return err
	}

	if err := workflow.Await(ctx, func() bool { return activated }); err != nil {
		return err
	}
	return activationErr
}

// Activate runs the first-period activation activities: charge → record event →
// fire payment + lifecycle hooks. Internal — invoked from the Activate update
// handler (no-trial case) and from the trial-end transition (trial-to-paid).
func (s *Subscription) Activate(ctx workflow.Context) error {
	if err := s.charge(ctx, "activation", 0); err != nil {
		return err
	}
	s.transitionTo(ctx, PhaseActive)
	_ = s.fireLifecycleHook(ctx, "subscription.activated")
	return nil
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/workflow/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/activation.go
git commit -m "feat(workflow): AwaitActivation update handler + Activate orchestration"
```

---

## Task 18: Workflow renewal phase

**Files:**
- Create: `internal/workflow/renewal.go`

- [ ] **Step 1: Create `internal/workflow/renewal.go`**

```go
package workflow

import (
	"go.temporal.io/sdk/workflow"
)

// Renew runs the renewal-period activities: charge → record event → fire
// payment + renewed hooks. Returns the original charge error so the caller
// (Run) can route to HandleDunning on failure.
func (s *Subscription) Renew(ctx workflow.Context) error {
	if err := s.charge(ctx, "renewal", 0); err != nil {
		return err
	}
	s.transitionTo(ctx, PhaseActive)
	_ = s.fireLifecycleHook(ctx, "subscription.renewed")
	return nil
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/workflow/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/renewal.go
git commit -m "feat(workflow): Renew runs renewal-period charge + lifecycle hook"
```

---

## Task 19: Workflow dunning loop

**Files:**
- Create: `internal/workflow/dunning.go`

- [ ] **Step 1: Create `internal/workflow/dunning.go`**

```go
package workflow

import (
	"errors"
	"time"

	"go.temporal.io/sdk/workflow"
)

// HandleDunning runs the retry loop after a failed renewal charge. Transitions
// to past_due on entry; on each retry calls charge() with the current
// DunningAttempt; recovers to active on success; returns ErrDunningExhausted
// if all attempts fail.
func (s *Subscription) HandleDunning(ctx workflow.Context) error {
	s.transitionTo(ctx, PhasePastDue)
	_ = s.fireLifecycleHook(ctx, "subscription.past_due")

	for s.DunningAttempt < s.Plan.DunningMaxAttempts {
		s.DunningAttempt++

		// Exponential backoff: initial * 2^(attempt-1).
		backoff := s.Plan.DunningRetryBackoff * (1 << (s.DunningAttempt - 1))
		_ = workflow.Sleep(ctx, backoff)

		if err := s.charge(ctx, "dunning", s.DunningAttempt); err == nil {
			// Recovered.
			s.DunningAttempt = 0
			s.transitionTo(ctx, PhaseActive)
			_ = s.fireLifecycleHook(ctx, "subscription.recovered")
			return nil
		}
		// Charge failed again — loop.
	}

	return ErrDunningExhausted
}

// ErrDunningExhausted indicates all dunning retries failed. The caller (Run)
// transitions the subscription to deactivated.
var ErrDunningExhausted = errors.New("dunning exhausted")

// Compile-time check: backoff arithmetic stays a time.Duration.
var _ = time.Duration(0)
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/workflow/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/dunning.go
git commit -m "feat(workflow): HandleDunning retry loop with exponential backoff and recovery"
```

---

## Task 20: Workflow cancellation, deactivation, continuation

**Files:**
- Create: `internal/workflow/cancellation.go`
- Create: `internal/workflow/continuation.go`

- [ ] **Step 1: Create `internal/workflow/cancellation.go`**

```go
package workflow

import (
	"go.temporal.io/sdk/workflow"
)

// AwaitPeriodEndOrCancellation blocks until either the period timer fires or
// a cancel signal arrives. End-of-period semantics: if cancel arrives early,
// the workflow sleeps the remainder before returning, then returns true.
// Returns true if canceled, false on natural period end.
func (s *Subscription) AwaitPeriodEndOrCancellation(ctx workflow.Context) bool {
	// If cancel was already requested (e.g., signal arrived during charge),
	// transition to canceled immediately and sleep out the period.
	if s.CancelRequested {
		s.transitionTo(ctx, PhaseCanceled)
		_ = s.fireLifecycleHook(ctx, "subscription.canceled")
		s.sleepRemainder(ctx)
		return true
	}

	now := workflow.Now(ctx)
	if !s.Period.End.After(now) {
		return s.CancelRequested
	}
	timer := workflow.NewTimer(ctx, s.Period.End.Sub(now))

	sel := workflow.NewSelector(ctx)
	periodEnded := false
	canceledDuringWait := false
	sel.AddFuture(timer, func(workflow.Future) { periodEnded = true })

	for !periodEnded && !canceledDuringWait {
		sel.Select(ctx)
		if s.CancelRequested && !canceledDuringWait {
			canceledDuringWait = true
			s.transitionTo(ctx, PhaseCanceled)
			_ = s.fireLifecycleHook(ctx, "subscription.canceled")
		}
	}

	if canceledDuringWait && !periodEnded {
		s.sleepRemainder(ctx)
		return true
	}
	return canceledDuringWait
}

// sleepRemainder durably sleeps until s.Period.End. No-op if past.
func (s *Subscription) sleepRemainder(ctx workflow.Context) {
	remaining := s.Period.End.Sub(workflow.Now(ctx))
	if remaining > 0 {
		_ = workflow.Sleep(ctx, remaining)
	}
}

// Deactivate runs the terminal deactivation hook and transitions to deactivated.
// After this returns, the workflow run completes (no CAN).
func (s *Subscription) Deactivate(ctx workflow.Context) error {
	s.transitionTo(ctx, PhaseDeactivated)
	_ = s.fireLifecycleHook(ctx, "subscription.deactivated")
	return nil
}
```

- [ ] **Step 2: Create `internal/workflow/continuation.go`**

```go
package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)

// ContinueIntoNextPeriod CANs the workflow with the next period's input.
// Upserts the SubflowPeriodEnd search attribute first because the new period
// boundary should be visible immediately for "expiring soon" queries.
func (s *Subscription) ContinueIntoNextPeriod(ctx workflow.Context) error {
	next := subscription.NextBillingPeriod(s.toInput())
	_ = workflow.UpsertSearchAttributes(ctx, map[string]any{
		subflowtemporal.AttrPeriodEnd: next.PeriodEnd,
	})
	return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, next)
}

// toInput serializes the entity's relevant state back into a
// SubscriptionInput for CAN. The next run reconstructs from this.
func (s *Subscription) toInput() subscription.SubscriptionInput {
	return subscription.SubscriptionInput{
		SubscriptionID:  s.ID,
		UserID:          s.UserID,
		PlanID:          s.PlanID,
		Plan:            s.Plan,
		PeriodStart:     s.Period.Start,
		PeriodEnd:       s.Period.End,
		Context:         s.Context.Clone(),
		RenewalCount:    s.RenewalCount,
		CancelRequested: s.CancelRequested,
	}
}
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/workflow/...
```

- [ ] **Step 4: Commit**

```bash
git add internal/workflow/cancellation.go internal/workflow/continuation.go
git commit -m "feat(workflow): AwaitPeriodEndOrCancellation + Deactivate + ContinueIntoNextPeriod"
```

---

## Task 21: Implement Run — wire all the pieces together

**Files:**
- Modify: `internal/workflow/subscription.go` (replace stub Run with real body)

- [ ] **Step 1: Replace Run in `internal/workflow/subscription.go`**

Find the stub `Run` method and replace with the full body. The full file should now look like:

```go
package workflow

import (
	"errors"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
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

type Subscription struct {
	ID       string
	UserID   string
	PlanID   string
	PlanCode string
	Plan     plan.Snapshot

	Phase           Phase
	Period          BillingPeriod
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool

	DunningAttempt int

	LastChargedAt         time.Time
	LastChargeAmountCents int64
	LastFailureAt         time.Time
	LastFailureReason     string
	TotalChargedCents     int64
	SuccessfulChargeCount int
	FailedChargeCount     int
}

func NewSubscription(in subscription.SubscriptionInput) *Subscription {
	return &Subscription{
		ID:              in.SubscriptionID,
		UserID:          in.UserID,
		PlanID:          in.PlanID,
		PlanCode:        in.Plan.Code,
		Plan:            in.Plan,
		Phase:           PhaseStarting,
		Period:          BillingPeriod{Start: in.PeriodStart, End: in.PeriodEnd},
		RenewalCount:    in.RenewalCount,
		Context:         in.Context.Clone(),
		CancelRequested: in.CancelRequested,
	}
}

func SubscriptionWorkflow(ctx workflow.Context, in subscription.SubscriptionInput) error {
	return NewSubscription(in).Run(ctx)
}

type ActivationResult struct {
	Phase   string
	Context subscription.Context
}

// Run is the entity workflow body. Branches on first-period (with or without
// trial) vs renewal; on renewal, routes to dunning on failure; awaits period
// end or cancel; deactivates or CANs into the next period.
func (s *Subscription) Run(ctx workflow.Context) error {
	if err := s.registerHandlers(ctx); err != nil {
		return err
	}

	if s.RenewalCount == 0 {
		// First-period path.
		if s.Plan.TrialDuration > 0 {
			outcome, err := s.Trial(ctx)
			if err != nil {
				return s.Deactivate(ctx)
			}
			if outcome == trialOutcomeCanceled {
				return s.Deactivate(ctx)
			}
		}
		if err := s.AwaitActivation(ctx); err != nil {
			return s.Deactivate(ctx)
		}
	} else {
		// Renewal path.
		if err := s.Renew(ctx); err != nil {
			if dunErr := s.HandleDunning(ctx); dunErr != nil {
				if errors.Is(dunErr, ErrDunningExhausted) {
					return s.Deactivate(ctx)
				}
				return dunErr
			}
		}
	}

	if cancelled := s.AwaitPeriodEndOrCancellation(ctx); cancelled {
		return s.Deactivate(ctx)
	}
	return s.ContinueIntoNextPeriod(ctx)
}
```

- [ ] **Step 2: Verify the package builds end-to-end**

```bash
go build ./internal/workflow/...
go build ./...
```

Expected: `internal/workflow/...` builds clean. `./...` may still fail on `cmd/api`, `cmd/worker`, and `internal/server/subscriptions.go` — those are addressed in T24-T27. That's expected at this point.

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/subscription.go
git commit -m "feat(workflow): wire Subscription.Run — trial/activation/renewal/dunning/cancel paths"
```

---

## Task 22: Workflow tests — 5 scenarios via testsuite

**Files:**
- Create: `internal/workflow/subscription_test.go`

- [ ] **Step 1: Create `internal/workflow/subscription_test.go`**

```go
package workflow

import (
	"testing"
	"time"

	activityPkg "github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// ---- shared test fixtures ----

func sampleInput(planMods ...func(*plan.Snapshot)) subscription.SubscriptionInput {
	start := time.Now().UTC().Truncate(time.Second)
	p := plan.Snapshot{
		Code:                "monthly-basic",
		Cadence:             30 * 24 * time.Hour,
		PriceCents:          999,
		Currency:            "USD",
		IntegrationEndpoint: "mock:50052",
		EnabledHooks: []string{
			"subscription.trial_started", "subscription.trial_will_end",
			"subscription.activated", "subscription.renewed",
			"subscription.past_due", "subscription.recovered",
			"subscription.canceled", "subscription.deactivated",
			"payment.succeeded", "payment.failed",
		},
	}
	for _, m := range planMods {
		m(&p)
	}
	return subscription.SubscriptionInput{
		SubscriptionID: "sub-1",
		UserID:         "user-1",
		PlanID:         "plan-1",
		Plan:           p,
		PeriodStart:    start,
		PeriodEnd:      start.Add(p.Cadence),
		Context:        subscription.Context{"card_id": "card_001"},
	}
}

type hookRecord struct {
	lifecycle map[string]int
	payment   map[string]int
}

func newHookRecord() *hookRecord {
	return &hookRecord{lifecycle: map[string]int{}, payment: map[string]int{}}
}

func registerMocks(env *testsuite.TestWorkflowEnvironment, rec *hookRecord, chargeBehavior func(attempt int) error) {
	chargeAttempts := 0
	env.RegisterActivityWithOptions(
		func(in activityPkg.ChargePaymentInput) (activityPkg.ChargePaymentResult, error) {
			chargeAttempts++
			if chargeBehavior != nil {
				if err := chargeBehavior(chargeAttempts); err != nil {
					return activityPkg.ChargePaymentResult{}, err
				}
			}
			return activityPkg.ChargePaymentResult{
				Reference: in.Reference, TransactionID: "txn", AmountCents: in.AmountCents, Currency: in.Currency,
			}, nil
		},
		activity.RegisterOptions{Name: "ChargePayment"},
	)
	env.RegisterActivityWithOptions(
		func(_ billing.Event) error { return nil },
		activity.RegisterOptions{Name: "RecordBillingEvent"},
	)

	// All 10 hooks: record by name, no-op.
	mkLifecycle := func(name string) func(activityPkg.LifecycleHookInput) error {
		return func(_ activityPkg.LifecycleHookInput) error {
			rec.lifecycle[name]++
			return nil
		}
	}
	mkPayment := func(name string) func(activityPkg.PaymentHookInput) error {
		return func(_ activityPkg.PaymentHookInput) error {
			rec.payment[name]++
			return nil
		}
	}
	env.RegisterActivityWithOptions(mkLifecycle("subscription.trial_started"), activity.RegisterOptions{Name: "OnTrialStarted"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.trial_will_end"), activity.RegisterOptions{Name: "OnTrialWillEnd"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.activated"), activity.RegisterOptions{Name: "OnActivated"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.renewed"), activity.RegisterOptions{Name: "OnRenewed"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.past_due"), activity.RegisterOptions{Name: "OnPastDue"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.recovered"), activity.RegisterOptions{Name: "OnRecovered"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.canceled"), activity.RegisterOptions{Name: "OnCanceled"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.deactivated"), activity.RegisterOptions{Name: "OnDeactivated"})
	env.RegisterActivityWithOptions(mkPayment("payment.succeeded"), activity.RegisterOptions{Name: "OnPaymentSucceeded"})
	env.RegisterActivityWithOptions(mkPayment("payment.failed"), activity.RegisterOptions{Name: "OnPaymentFailed"})
}

// ---- tests ----

func TestSubscription_HappyActivation_ContinuesAsNew(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	// Send the Activate update at t=0 so activation can complete.
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "act-1", t)
	}, 0)

	env.ExecuteWorkflow(SubscriptionWorkflow, sampleInput())

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	// ContinueAsNew is surfaced as a non-nil error.
	if err := env.GetWorkflowError(); err == nil {
		t.Fatalf("expected ContinueAsNew error, got nil")
	}
	if rec.lifecycle["subscription.activated"] != 1 {
		t.Fatalf("expected 1 activated hook, got %d", rec.lifecycle["subscription.activated"])
	}
	if rec.payment["payment.succeeded"] != 1 {
		t.Fatalf("expected 1 payment.succeeded hook, got %d", rec.payment["payment.succeeded"])
	}
}

func TestSubscription_TrialThenActivate(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	input := sampleInput(func(p *plan.Snapshot) {
		p.TrialDuration = 24 * time.Hour
		p.TrialEndNoticeBefore = 2 * time.Hour
		// Re-anchor period to the trial duration for clarity.
	})
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.TrialDuration)

	// Send Activate after the trial ends (just over 24h).
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "act-1", t)
	}, 24*time.Hour+10*time.Minute)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.trial_started"] != 1 {
		t.Fatalf("expected trial_started, got %d", rec.lifecycle["subscription.trial_started"])
	}
	if rec.lifecycle["subscription.trial_will_end"] != 1 {
		t.Fatalf("expected trial_will_end, got %d", rec.lifecycle["subscription.trial_will_end"])
	}
	if rec.lifecycle["subscription.activated"] != 1 {
		t.Fatalf("expected activated, got %d", rec.lifecycle["subscription.activated"])
	}
}

func TestSubscription_TrialCanceled(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	input := sampleInput(func(p *plan.Snapshot) {
		p.TrialDuration = 24 * time.Hour
	})
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.TrialDuration)

	// Cancel during trial.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancelSubscription, struct{}{})
	}, 6*time.Hour)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if rec.payment["payment.succeeded"] != 0 {
		t.Fatalf("trial-canceled should not have charged")
	}
	if rec.lifecycle["subscription.deactivated"] != 1 {
		t.Fatalf("expected deactivated, got %d", rec.lifecycle["subscription.deactivated"])
	}
}

func TestSubscription_RenewalDunningRecovery(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()

	// Renewal first charge fails (one transient), then recovers on dunning attempt 1.
	registerMocks(env, rec, func(attempt int) error {
		if attempt == 1 {
			return temporal.NewApplicationError("transient", "PaymentGatewayTimeoutError")
		}
		return nil
	})

	input := sampleInput(func(p *plan.Snapshot) {
		p.DunningMaxAttempts = 3
		p.DunningRetryBackoff = 1 * time.Hour
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.past_due"] != 1 {
		t.Fatalf("expected past_due, got %d", rec.lifecycle["subscription.past_due"])
	}
	if rec.lifecycle["subscription.recovered"] != 1 {
		t.Fatalf("expected recovered, got %d", rec.lifecycle["subscription.recovered"])
	}
}

func TestSubscription_RenewalDunningExhausted(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()

	// Every charge fails with a transient error so dunning exhausts.
	registerMocks(env, rec, func(_ int) error {
		return temporal.NewApplicationError("transient", "PaymentGatewayTimeoutError")
	})

	input := sampleInput(func(p *plan.Snapshot) {
		p.DunningMaxAttempts = 2
		p.DunningRetryBackoff = 1 * time.Hour
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.deactivated"] != 1 {
		t.Fatalf("expected deactivated, got %d", rec.lifecycle["subscription.deactivated"])
	}
}
```

- [ ] **Step 2: Run tests**

```bash
go test ./internal/workflow/... -v -run TestSubscription 2>&1 | tail -40
```

Expected: all 5 tests PASS.

If any tests fail, do not paper over them — investigate the actual lifecycle / hook timing. Common pitfalls:
- Test clock skew: `sampleInput` uses `time.Now()` so the period is in the future relative to the harness's wall-clock start.
- Forgetting to `env.UpdateWorkflowNoRejection(UpdateActivate, ...)` in tests that go through activation.
- Mock activity returning a retryable error when test expects terminal behavior.

- [ ] **Step 3: Commit**

```bash
git add internal/workflow/subscription_test.go
git commit -m "test(workflow): 5 lifecycle scenarios (activation, trial, trial-cancel, dunning recovery, dunning exhausted)"
```

---

## Task 23: Server — plans handler validation

**Files:**
- Modify: `internal/server/plans.go`

- [ ] **Step 1: Rewrite `internal/server/plans.go`**

```go
package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PlanService struct {
	Repo *store.PlanRepository
}

func (s *PlanService) CreatePlan(ctx context.Context, req *subflowv1.CreatePlanRequest) (*subflowv1.Plan, error) {
	p, err := buildPlanFromRequest(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := s.Repo.Insert(ctx, p); err != nil {
		return nil, status.Errorf(codes.Internal, "insert plan: %v", err)
	}
	return planToProto(p), nil
}

func (s *PlanService) GetPlan(ctx context.Context, req *subflowv1.GetPlanRequest) (*subflowv1.Plan, error) {
	p, err := s.Repo.Get(ctx, req.Id)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}
	return planToProto(p), nil
}

func (s *PlanService) ListPlans(ctx context.Context, _ *subflowv1.ListPlansRequest) (*subflowv1.ListPlansResponse, error) {
	plans, err := s.Repo.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list plans: %v", err)
	}
	out := &subflowv1.ListPlansResponse{Plans: make([]*subflowv1.Plan, 0, len(plans))}
	for _, p := range plans {
		out.Plans = append(out.Plans, planToProto(p))
	}
	return out, nil
}

func (s *PlanService) DeletePlan(ctx context.Context, req *subflowv1.DeletePlanRequest) (*subflowv1.DeletePlanResponse, error) {
	if err := s.Repo.Delete(ctx, req.Id); err != nil {
		if errors.Is(err, store.ErrPlanNotFound) {
			return nil, status.Error(codes.NotFound, "plan not found")
		}
		return nil, status.Errorf(codes.Internal, "delete plan: %v", err)
	}
	return &subflowv1.DeletePlanResponse{}, nil
}

// buildPlanFromRequest applies defaults, validates, and returns a domain Plan.
func buildPlanFromRequest(req *subflowv1.CreatePlanRequest) (plan.Plan, error) {
	cadence, err := time.ParseDuration(req.Cadence)
	if err != nil || cadence <= 0 {
		return plan.Plan{}, fmt.Errorf("cadence %q invalid (must be positive Go duration)", req.Cadence)
	}
	trial, err := parseOptionalDuration(req.TrialDuration)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("trial_duration: %w", err)
	}
	notice, err := parseOptionalDuration(req.TrialEndNoticeBefore)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("trial_end_notice_before: %w", err)
	}
	if notice > 0 && trial > 0 && notice >= trial {
		return plan.Plan{}, fmt.Errorf("trial_end_notice_before (%s) must be less than trial_duration (%s)", notice, trial)
	}
	backoff, err := parseOptionalDuration(req.DunningRetryBackoff)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("dunning_retry_backoff: %w", err)
	}

	if req.DunningMaxAttempts < 0 || req.DunningMaxAttempts > 99 {
		return plan.Plan{}, fmt.Errorf("dunning_max_attempts %d out of range [0,99]", req.DunningMaxAttempts)
	}
	if req.PriceCents < 0 {
		return plan.Plan{}, fmt.Errorf("price_cents must be >= 0")
	}
	if req.PerUserLimit < 0 {
		return plan.Plan{}, fmt.Errorf("per_user_limit must be >= 0 (0 = unlimited)")
	}

	hooks, err := plan.ParseHookNames(req.EnabledHooks)
	if err != nil {
		return plan.Plan{}, err
	}

	currency := strings.ToUpper(req.Currency)
	if currency == "" {
		currency = "USD"
	}
	perUserLimit := int(req.PerUserLimit)
	// proto3 can't distinguish "unset" from "0"; we treat 0 as the unlimited
	// intent (documented in the API). Callers wanting "exactly 1" pass 1.
	dunningMaxAttempts := int(req.DunningMaxAttempts)
	if dunningMaxAttempts == 0 {
		dunningMaxAttempts = 3 // default when caller didn't set the field
	}
	if backoff == 0 {
		backoff = 24 * time.Hour
	}

	return plan.Plan{
		ID:                   uuid.NewString(),
		Code:                 req.Code,
		Name:                 req.Name,
		Cadence:              cadence,
		PriceCents:           req.PriceCents,
		Currency:             currency,
		PerUserLimit:         perUserLimit,
		TrialDuration:        trial,
		TrialEndNoticeBefore: notice,
		DunningMaxAttempts:   dunningMaxAttempts,
		DunningRetryBackoff:  backoff,
		IntegrationEndpoint:  req.IntegrationEndpoint,
		EnabledHooks:         hooks,
		CreatedAt:            time.Now().UTC(),
	}, nil
}

// parseOptionalDuration is the single boundary parser for optional duration
// strings. Empty input = zero duration; non-empty must parse cleanly.
func parseOptionalDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func planToProto(p plan.Plan) *subflowv1.Plan {
	hookStrings := make([]string, len(p.EnabledHooks))
	for i, h := range p.EnabledHooks {
		hookStrings[i] = string(h)
	}
	return &subflowv1.Plan{
		Id:                   p.ID,
		Code:                 p.Code,
		Name:                 p.Name,
		Cadence:              p.Cadence.String(),
		PriceCents:           p.PriceCents,
		Currency:             p.Currency,
		PerUserLimit:         int32(p.PerUserLimit),
		TrialDuration:        durationOrEmpty(p.TrialDuration),
		TrialEndNoticeBefore: durationOrEmpty(p.TrialEndNoticeBefore),
		DunningMaxAttempts:   int32(p.DunningMaxAttempts),
		DunningRetryBackoff:  durationOrEmpty(p.DunningRetryBackoff),
		IntegrationEndpoint:  p.IntegrationEndpoint,
		EnabledHooks:         hookStrings,
	}
}

// durationOrEmpty is the boundary formatter for optional durations. Zero =
// empty string on the wire; non-zero uses Go's standard duration format.
func durationOrEmpty(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/server/...
```

Note: this will fail on `subscriptions.go` (still references the deleted projection). Task 24 fixes it.

- [ ] **Step 3: Commit**

```bash
git add internal/server/plans.go
git commit -m "feat(server): plan create validation for all new fields + ValidHookNames check"
```

---

## Task 24: Server — subscription handlers (Visibility-based listing, new endpoints)

**Files:**
- Rewrite: `internal/server/subscriptions.go`

- [ ] **Step 1: Rewrite `internal/server/subscriptions.go`**

```go
package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/store"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"github.com/martavoi/subflow/internal/workflow"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SubscriptionService struct {
	Temporal  client.Client
	Namespace string
	TaskQueue string
	PlanRepo  *store.PlanRepository
}

// CreateSubscription enforces per-user limit then starts the workflow with
// the Activate update in the same round trip, so the customer learns the
// outcome (active, trialing, or FailedPrecondition) before the RPC returns.
func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *subflowv1.CreateSubscriptionRequest) (*subflowv1.Subscription, error) {
	p, err := s.PlanRepo.Get(ctx, req.PlanId)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}

	if p.PerUserLimit > 0 {
		count, err := s.countActiveForUser(ctx, req.UserId, p.Code)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "count workflows: %v", err)
		}
		if count >= int64(p.PerUserLimit) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"per-user subscription limit reached for plan %q (%d/%d)",
				p.Code, count, p.PerUserLimit)
		}
	}

	now := time.Now().UTC()
	subID := uuid.NewString()
	periodEnd := now.Add(p.Cadence)
	if p.TrialDuration > 0 {
		periodEnd = now.Add(p.TrialDuration)
	}

	wfInput := subscription.SubscriptionInput{
		SubscriptionID: subID,
		UserID:         req.UserId,
		PlanID:         p.ID,
		Plan:           plan.SnapshotOf(p),
		PeriodStart:    now,
		PeriodEnd:      periodEnd,
		Context:        subscription.Context(req.InitialContext),
	}

	initialPhase := "active"
	if p.TrialDuration > 0 {
		initialPhase = "trialing"
	}

	startSA := map[string]any{
		subflowtemporal.AttrUserId:    req.UserId,
		subflowtemporal.AttrPlanCode:  p.Code,
		subflowtemporal.AttrPhase:     initialPhase,
		subflowtemporal.AttrPeriodEnd: periodEnd,
	}
	if p.TrialDuration > 0 {
		startSA[subflowtemporal.AttrTrialEnd] = periodEnd
	}

	startOp := s.Temporal.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       "subscription:" + subID,
		TaskQueue:                s.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		SearchAttributes:         startSA,
	}, workflow.SubscriptionWorkflow, wfInput)

	// If the plan has a trial, we still call UpdateWithStartWorkflow but the
	// workflow won't process the Activate update until after the trial ends.
	// In the trial-during-creation case we don't want to block — so we use
	// WaitForStage Accepted for trial flows, Completed for no-trial flows.
	waitFor := client.WorkflowUpdateStageCompleted
	if p.TrialDuration > 0 {
		waitFor = client.WorkflowUpdateStageAccepted
	}

	handle, err := s.Temporal.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName:   workflow.UpdateActivate,
			WaitForStage: waitFor,
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start workflow: %v", err)
	}

	// For non-trial plans we wait for the activation to complete and use the
	// result to populate the response. For trial plans we just return the
	// initial state — the workflow is now running in trialing phase.
	resp := &subflowv1.Subscription{
		Id:          subID,
		UserId:      req.UserId,
		PlanCode:    p.Code,
		Phase:       initialPhase,
		PeriodStart: timestamppb.New(now),
		PeriodEnd:   timestamppb.New(periodEnd),
		Context:     map[string]string(wfInput.Context),
	}
	if p.TrialDuration == 0 {
		var result workflow.ActivationResult
		if err := handle.Get(ctx, &result); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "activation failed: %v", err)
		}
		resp.Phase = result.Phase
		resp.Context = map[string]string(result.Context)
	}
	return resp, nil
}

func (s *SubscriptionService) CancelSubscription(ctx context.Context, req *subflowv1.CancelSubscriptionRequest) (*subflowv1.CancelSubscriptionResponse, error) {
	if err := s.Temporal.SignalWorkflow(ctx, "subscription:"+req.Id, "", workflow.SignalCancelSubscription, struct{}{}); err != nil {
		return nil, status.Errorf(codes.NotFound, "signal workflow: %v", err)
	}
	return &subflowv1.CancelSubscriptionResponse{}, nil
}

func (s *SubscriptionService) UpdateSubscriptionContext(ctx context.Context, req *subflowv1.UpdateSubscriptionContextRequest) (*subflowv1.Subscription, error) {
	if err := s.Temporal.SignalWorkflow(ctx, "subscription:"+req.Id, "", workflow.SignalUpdateContext, req.Updates); err != nil {
		return nil, status.Errorf(codes.NotFound, "signal workflow: %v", err)
	}
	return s.GetSubscription(ctx, &subflowv1.GetSubscriptionRequest{Id: req.Id})
}

func (s *SubscriptionService) GetSubscription(ctx context.Context, req *subflowv1.GetSubscriptionRequest) (*subflowv1.Subscription, error) {
	res, err := s.Temporal.QueryWorkflow(ctx, "subscription:"+req.Id, "", workflow.QuerySubscriptionStatus)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "subscription not found: %v", err)
	}
	var st workflow.Status
	if err := res.Get(&st); err != nil {
		return nil, status.Errorf(codes.Internal, "decode status: %v", err)
	}
	return statusToProto(req.Id, st), nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, req *subflowv1.ListSubscriptionsRequest) (*subflowv1.ListSubscriptionsResponse, error) {
	query := buildListQuery(req)
	pageSize := int32(req.PageSize)
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 200 {
		pageSize = 200
	}
	resp, err := s.Temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     s.Namespace,
		Query:         query,
		PageSize:      pageSize,
		NextPageToken: []byte(req.PageToken),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflows: %v", err)
	}

	out := &subflowv1.ListSubscriptionsResponse{
		Subscriptions: make([]*subflowv1.Subscription, 0, len(resp.Executions)),
		NextPageToken: string(resp.NextPageToken),
	}
	for _, exec := range resp.Executions {
		subID := strings.TrimPrefix(exec.Execution.WorkflowId, "subscription:")
		full, err := s.GetSubscription(ctx, &subflowv1.GetSubscriptionRequest{Id: subID})
		if err != nil {
			continue // workflow may have just closed; skip
		}
		out.Subscriptions = append(out.Subscriptions, full)
	}
	return out, nil
}

func (s *SubscriptionService) countActiveForUser(ctx context.Context, userID, planCode string) (int64, error) {
	q := fmt.Sprintf(
		"WorkflowType='SubscriptionWorkflow' AND %s='%s' AND %s='%s' AND %s IN ('trialing','active','past_due','canceled')",
		subflowtemporal.AttrUserId, userID,
		subflowtemporal.AttrPlanCode, planCode,
		subflowtemporal.AttrPhase,
	)
	resp, err := s.Temporal.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.Namespace,
		Query:     q,
	})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func buildListQuery(req *subflowv1.ListSubscriptionsRequest) string {
	parts := []string{"WorkflowType='SubscriptionWorkflow'"}
	if req.UserId != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'", subflowtemporal.AttrUserId, req.UserId))
	}
	if req.PlanCode != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'", subflowtemporal.AttrPlanCode, req.PlanCode))
	}
	if req.Phase != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'", subflowtemporal.AttrPhase, req.Phase))
	}
	return strings.Join(parts, " AND ")
}

func statusToProto(id string, st workflow.Status) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:                    id,
		UserId:                "",  // GetSubscription via query; user_id is available in SA but not in Status. Augmented from Visibility for List path.
		PlanCode:              "",  // same — see above
		Phase:                 st.Phase,
		PeriodStart:           timestamppb.New(st.PeriodStart),
		PeriodEnd:             timestamppb.New(st.PeriodEnd),
		RenewalCount:          int32(st.RenewalCount),
		Context:               map[string]string(st.Context),
		CancelRequested:       st.CancelRequested,
		DunningAttempt:        int32(st.DunningAttempt),
		LastChargedAt:         timestamppb.New(st.LastChargedAt),
		LastChargeAmountCents: st.LastChargeAmountCents,
		LastFailureAt:         timestamppb.New(st.LastFailureAt),
		LastFailureReason:     st.LastFailureReason,
		TotalChargedCents:     st.TotalChargedCents,
		SuccessfulChargeCount: int32(st.SuccessfulChargeCount),
		FailedChargeCount:     int32(st.FailedChargeCount),
	}
}
```

**Note:** `Subscription.user_id` and `plan_code` come back as empty strings in this implementation because the workflow Query doesn't return them (they're available via search attributes). For the POC this is acceptable. If you want them populated, modify `workflow.Status` to include them and `(*Subscription).AsStatus` to populate them (the entity already holds both fields).

- [ ] **Step 2: Augment workflow.Status to carry user_id + plan_code**

Edit `internal/workflow/status.go` — add fields:

```go
type Status struct {
	UserID                string
	PlanCode              string
	Phase                 string
	PeriodStart           time.Time
	PeriodEnd             time.Time
	RenewalCount          int
	Context               subscription.Context
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

Edit `internal/workflow/handlers.go` — populate UserID + PlanCode in `AsStatus`:

```go
func (s *Subscription) AsStatus() (Status, error) {
	return Status{
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

Edit `internal/server/subscriptions.go` — populate UserId + PlanCode in `statusToProto`:

```go
func statusToProto(id string, st workflow.Status) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:                    id,
		UserId:                st.UserID,
		PlanCode:              st.PlanCode,
		Phase:                 st.Phase,
		// ... (rest unchanged)
	}
}
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/workflow/... ./internal/server/...
```

- [ ] **Step 4: Commit**

```bash
git add internal/server/subscriptions.go internal/workflow/status.go internal/workflow/handlers.go
git commit -m "feat(server): Visibility-based ListSubscriptions, per-user-limit via CountWorkflow, UpdateSubscriptionContext"
```

---

## Task 25: Server — billing events handler

**Files:**
- Create: `internal/server/billing_events.go`

- [ ] **Step 1: Create `internal/server/billing_events.go`**

```go
package server

import (
	"context"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/billing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BillingEventsService struct {
	Events billing.EventStore
}

func (s *BillingEventsService) ListBillingEvents(ctx context.Context, req *subflowv1.ListBillingEventsRequest) (*subflowv1.ListBillingEventsResponse, error) {
	events, nextCursor, err := s.Events.List(ctx, billing.ListQuery{
		SubscriptionID: req.SubscriptionId,
		UserID:         req.UserId,
		TypeFilter:     req.TypeFilter,
		PageSize:       int(req.PageSize),
		PageCursor:     req.PageCursor,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list billing events: %v", err)
	}
	out := &subflowv1.ListBillingEventsResponse{
		Events:     make([]*subflowv1.BillingEvent, 0, len(events)),
		NextCursor: nextCursor,
	}
	for _, ev := range events {
		out.Events = append(out.Events, &subflowv1.BillingEvent{
			Id:             ev.ID,
			SubscriptionId: ev.SubscriptionID,
			UserId:         ev.UserID,
			PlanCode:       ev.PlanCode,
			Type:           ev.Type,
			AmountCents:    ev.AmountCents,
			Currency:       ev.Currency,
			PeriodStart:    timestamppb.New(ev.PeriodStart),
			PeriodEnd:      timestamppb.New(ev.PeriodEnd),
			RenewalCount:   int32(ev.RenewalCount),
			DunningAttempt: int32(ev.DunningAttempt),
			TransactionId:  ev.TransactionID,
			FailureReason:  ev.FailureReason,
			OccurredAt:     timestamppb.New(ev.OccurredAt),
		})
	}
	return out, nil
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/server/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/server/billing_events.go
git commit -m "feat(server): ListBillingEvents reads from EventStore directly"
```

---

## Task 26: cmd/worker — register new activity set, drop projection

**Files:**
- Rewrite: `cmd/worker/main.go`

- [ ] **Step 1: Rewrite `cmd/worker/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/config"
	"github.com/martavoi/subflow/internal/integration"
	"github.com/martavoi/subflow/internal/store"
	wfpkg "github.com/martavoi/subflow/internal/workflow"
	tactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadWorker()
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, db, err := store.Connect(mongoCtx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		logger.Error("mongo connect", "err", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(context.Background())

	billingStore := billing.NewMongoStore(db)
	if err := billingStore.EnsureIndexes(ctx); err != nil {
		logger.Error("billing indexes", "err", err)
		os.Exit(1)
	}

	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		logger.Error("temporal dial", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	intClient := integration.NewClient()
	defer intClient.Close()

	paymentActs := &activity.PaymentActivities{
		TransientFailureRate: cfg.PaymentTransientRate,
		TerminalFailureRate:  cfg.PaymentTerminalRate,
	}
	billingActs := &activity.BillingActivities{Events: billingStore}
	hookActs := &activity.HookActivities{Client: intClient}

	w := worker.New(tc, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(wfpkg.SubscriptionWorkflow)

	// Core activities
	w.RegisterActivityWithOptions(paymentActs.ChargePayment, tactivity.RegisterOptions{Name: "ChargePayment"})
	w.RegisterActivityWithOptions(billingActs.RecordBillingEvent, tactivity.RegisterOptions{Name: "RecordBillingEvent"})

	// Hook activities (10)
	w.RegisterActivityWithOptions(hookActs.OnTrialStarted, tactivity.RegisterOptions{Name: "OnTrialStarted"})
	w.RegisterActivityWithOptions(hookActs.OnTrialWillEnd, tactivity.RegisterOptions{Name: "OnTrialWillEnd"})
	w.RegisterActivityWithOptions(hookActs.OnActivated, tactivity.RegisterOptions{Name: "OnActivated"})
	w.RegisterActivityWithOptions(hookActs.OnRenewed, tactivity.RegisterOptions{Name: "OnRenewed"})
	w.RegisterActivityWithOptions(hookActs.OnPastDue, tactivity.RegisterOptions{Name: "OnPastDue"})
	w.RegisterActivityWithOptions(hookActs.OnRecovered, tactivity.RegisterOptions{Name: "OnRecovered"})
	w.RegisterActivityWithOptions(hookActs.OnCanceled, tactivity.RegisterOptions{Name: "OnCanceled"})
	w.RegisterActivityWithOptions(hookActs.OnDeactivated, tactivity.RegisterOptions{Name: "OnDeactivated"})
	w.RegisterActivityWithOptions(hookActs.OnPaymentSucceeded, tactivity.RegisterOptions{Name: "OnPaymentSucceeded"})
	w.RegisterActivityWithOptions(hookActs.OnPaymentFailed, tactivity.RegisterOptions{Name: "OnPaymentFailed"})

	logger.Info("subflow-worker starting", "task_queue", cfg.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		logger.Error("worker run", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build -o /tmp/subflow-worker ./cmd/worker
```

- [ ] **Step 3: Commit**

```bash
git add cmd/worker/main.go
git commit -m "feat(cmd/worker): register charge/billing/10-hook activities; drop projection wiring"
```

---

## Task 27: cmd/api — SA bootstrapper, billing service wiring

**Files:**
- Rewrite: `cmd/api/main.go`

- [ ] **Step 1: Rewrite `cmd/api/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/config"
	"github.com/martavoi/subflow/internal/server"
	"github.com/martavoi/subflow/internal/store"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// AggregateService composes plan + subscription + billing services into the
// single SubflowService gRPC implementation.
type AggregateService struct {
	*server.PlanService
	*server.SubscriptionService
	*server.BillingEventsService
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadAPI()
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Mongo
	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, db, err := store.Connect(mongoCtx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		logger.Error("mongo connect", "err", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(context.Background())

	planRepo := store.NewPlanRepository(db)
	if err := planRepo.EnsureIndexes(ctx); err != nil {
		logger.Error("plan indexes", "err", err)
		os.Exit(1)
	}

	billingStore := billing.NewMongoStore(db)
	if err := billingStore.EnsureIndexes(ctx); err != nil {
		logger.Error("billing indexes", "err", err)
		os.Exit(1)
	}

	// Temporal
	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		logger.Error("temporal dial", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	// Register custom search attributes (idempotent).
	saCtx, saCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := subflowtemporal.EnsureSearchAttributes(saCtx, tc, cfg.TemporalNamespace, logger); err != nil {
		saCancel()
		logger.Error("ensure search attributes", "err", err)
		os.Exit(1)
	}
	saCancel()

	// gRPC server
	svc := &AggregateService{
		PlanService: &server.PlanService{Repo: planRepo},
		SubscriptionService: &server.SubscriptionService{
			Temporal:  tc,
			Namespace: cfg.TemporalNamespace,
			TaskQueue: cfg.TaskQueue,
			PlanRepo:  planRepo,
		},
		BillingEventsService: &server.BillingEventsService{Events: billingStore},
	}

	g := grpc.NewServer()
	subflowv1.RegisterSubflowServiceServer(g, svc)
	reflection.Register(g)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}
	logger.Info("subflow-api listening", "port", cfg.GRPCPort)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		g.GracefulStop()
	}()

	if err := g.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build -o /tmp/subflow-api ./cmd/api
go build ./...
```

Expected: ./... now builds clean except for `cmd/mock-integration` (which the next task rewrites).

- [ ] **Step 3: Commit**

```bash
git add cmd/api/main.go
git commit -m "feat(cmd/api): EnsureSearchAttributes at startup; BillingEventsService wired; no projection"
```

---

## Task 28: cmd/mock-integration — implement SubscriptionHooks

**Files:**
- Rewrite: `cmd/mock-integration/main.go`

- [ ] **Step 1: Rewrite `cmd/mock-integration/main.go`**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	subflowv1.UnimplementedSubscriptionHooksServer

	failureRate         float64
	terminalFailureRate float64
	latency             time.Duration
	logger              *slog.Logger

	mu      sync.Mutex
	seenRef map[string]bool // idempotency cache keyed by reference
}

func (s *server) maybeFail(hook, reference string) error {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	s.mu.Lock()
	if s.seenRef[reference] {
		s.mu.Unlock()
		s.logger.Info("idempotent replay", "hook", hook, "reference", reference)
		return nil
	}
	s.mu.Unlock()

	r := rand.Float64()
	if r < s.terminalFailureRate {
		s.logger.Warn("injected terminal", "hook", hook, "reference", reference)
		return status.Error(codes.FailedPrecondition, "injected terminal failure")
	}
	if r < s.terminalFailureRate+s.failureRate {
		s.logger.Warn("injected transient", "hook", hook, "reference", reference)
		return status.Error(codes.Unavailable, "injected transient failure")
	}

	s.mu.Lock()
	s.seenRef[reference] = true
	s.mu.Unlock()
	s.logger.Info("hook handled", "hook", hook, "reference", reference)
	return nil
}

func (s *server) OnTrialStarted(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnTrialStarted", ev.Reference))
}
func (s *server) OnTrialWillEnd(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnTrialWillEnd", ev.Reference))
}
func (s *server) OnActivated(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnActivated", ev.Reference))
}
func (s *server) OnRenewed(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnRenewed", ev.Reference))
}
func (s *server) OnPastDue(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnPastDue", ev.Reference))
}
func (s *server) OnRecovered(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnRecovered", ev.Reference))
}
func (s *server) OnCanceled(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnCanceled", ev.Reference))
}
func (s *server) OnDeactivated(_ context.Context, ev *subflowv1.LifecycleEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnDeactivated", ev.Reference))
}
func (s *server) OnPaymentSucceeded(_ context.Context, ev *subflowv1.PaymentEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnPaymentSucceeded", ev.Reference))
}
func (s *server) OnPaymentFailed(_ context.Context, ev *subflowv1.PaymentEvent) (*subflowv1.HookAck, error) {
	return ack(s.maybeFail("OnPaymentFailed", ev.Reference))
}

func ack(err error) (*subflowv1.HookAck, error) {
	if err != nil {
		return nil, err
	}
	return &subflowv1.HookAck{}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := getenv("MOCK_GRPC_PORT", "50052")
	failureRate := mustFloat(getenv("FAILURE_RATE", "0.0"))
	terminalRate := mustFloat(getenv("TERMINAL_FAILURE_RATE", "0.0"))
	latencyMs := mustInt(getenv("LATENCY_MS", "0"))

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	s := &server{
		failureRate:         failureRate,
		terminalFailureRate: terminalRate,
		latency:             time.Duration(latencyMs) * time.Millisecond,
		logger:              logger,
		seenRef:             make(map[string]bool),
	}

	g := grpc.NewServer()
	subflowv1.RegisterSubscriptionHooksServer(g, s)

	logger.Info("mock-integration listening", "port", port,
		"failure_rate", failureRate, "terminal_failure_rate", terminalRate, "latency_ms", latencyMs)
	if err := g.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Errorf("parse float %q: %w", s, err))
	}
	return f
}

func mustInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Errorf("parse int %q: %w", s, err))
	}
	return n
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build -o /tmp/subflow-mock ./cmd/mock-integration
go build ./...
go test ./...
```

Expected: full build clean. All unit tests pass.

- [ ] **Step 3: Commit**

```bash
git add cmd/mock-integration/main.go
git commit -m "feat(cmd/mock-integration): implement SubscriptionHooks (10 methods) with failure/latency knobs"
```

---

## Task 29: Taskfile + compose updates

**Files:**
- Modify: `Taskfile.yml`
- Keep: `compose.yml` (no structural changes needed)

- [ ] **Step 1: Rewrite `Taskfile.yml`**

```yaml
version: '3'

vars:
  API_HOST: localhost:50051

env:
  CGO_ENABLED: "0"

tasks:
  proto-gen:
    desc: Regenerate protobuf code
    cmds:
      - buf generate

  build:
    desc: Build all binaries to ./bin
    cmds:
      - go build -o bin/api ./cmd/api
      - go build -o bin/worker ./cmd/worker
      - go build -o bin/mock-integration ./cmd/mock-integration

  test:
    desc: Run all tests
    cmds:
      - go test ./... -v

  up:
    desc: Bring up the stack (podman compose)
    cmds:
      - podman compose up -d --build

  down:
    desc: Stop and remove containers (preserves volumes)
    cmds:
      - podman compose down

  reset:
    desc: Stop and wipe volumes (Mongo + Temporal SQLite)
    cmds:
      - podman compose down -v

  logs:
    desc: Tail logs of all services
    cmds:
      - podman compose logs -f

  ui:
    desc: Open the Temporal Web UI
    cmds:
      - open http://localhost:8233

  list-interfaces:
    desc: List exposed gRPC services
    cmds:
      - grpcurl -plaintext {{.API_HOST}} list

  seed-plan-monthly:
    desc: Create a "monthly-basic" plan, 30s cadence, all hooks enabled
    cmds:
      - |
        grpcurl -plaintext -d '{
          "code": "monthly-basic",
          "name": "Monthly Basic",
          "cadence": "30s",
          "price_cents": 999,
          "currency": "USD",
          "per_user_limit": 1,
          "dunning_max_attempts": 3,
          "dunning_retry_backoff": "5s",
          "integration_endpoint": "mock-integration:50052",
          "enabled_hooks": [
            "subscription.trial_started","subscription.trial_will_end",
            "subscription.activated","subscription.renewed",
            "subscription.past_due","subscription.recovered",
            "subscription.canceled","subscription.deactivated",
            "payment.succeeded","payment.failed"
          ]
        }' {{.API_HOST}} subflow.v1.SubflowService/CreatePlan

  seed-plan-trial:
    desc: Create a "trial-plan" with 30s trial, 60s cadence
    cmds:
      - |
        grpcurl -plaintext -d '{
          "code": "trial-plan",
          "name": "Trial Plan",
          "cadence": "60s",
          "price_cents": 1499,
          "currency": "USD",
          "per_user_limit": 1,
          "trial_duration": "30s",
          "trial_end_notice_before": "10s",
          "dunning_max_attempts": 2,
          "dunning_retry_backoff": "5s",
          "integration_endpoint": "mock-integration:50052",
          "enabled_hooks": [
            "subscription.trial_started","subscription.trial_will_end",
            "subscription.activated","subscription.renewed",
            "subscription.past_due","subscription.recovered",
            "subscription.canceled","subscription.deactivated",
            "payment.succeeded","payment.failed"
          ]
        }' {{.API_HOST}} subflow.v1.SubflowService/CreatePlan

  list-plans:
    desc: List all plans
    cmds:
      - grpcurl -plaintext {{.API_HOST}} subflow.v1.SubflowService/ListPlans

  create-subscription:
    desc: 'Create a subscription. Vars: USER, PLAN_ID'
    cmds:
      - |
        grpcurl -plaintext -d '{"user_id":"{{.USER}}","plan_id":"{{.PLAN_ID}}","initial_context":{"card_id":"card_001"}}' \
          {{.API_HOST}} subflow.v1.SubflowService/CreateSubscription
    requires: { vars: [USER, PLAN_ID] }

  cancel-subscription:
    desc: 'Cancel a subscription. Var: ID'
    cmds:
      - grpcurl -plaintext -d '{"id":"{{.ID}}"}' {{.API_HOST}} subflow.v1.SubflowService/CancelSubscription
    requires: { vars: [ID] }

  get-subscription:
    desc: 'Get subscription state. Var: ID'
    cmds:
      - grpcurl -plaintext -d '{"id":"{{.ID}}"}' {{.API_HOST}} subflow.v1.SubflowService/GetSubscription
    requires: { vars: [ID] }

  list-subscriptions:
    desc: 'List subscriptions. Optional vars: USER, PHASE, PLAN_CODE'
    cmds:
      - |
        grpcurl -plaintext -d '{"user_id":"{{.USER}}","phase":"{{.PHASE}}","plan_code":"{{.PLAN_CODE}}"}' \
          {{.API_HOST}} subflow.v1.SubflowService/ListSubscriptions

  list-billing-events:
    desc: 'List billing events. Optional vars: SUB_ID, USER, TYPE'
    cmds:
      - |
        grpcurl -plaintext -d '{"subscription_id":"{{.SUB_ID}}","user_id":"{{.USER}}","type_filter":"{{.TYPE}}","page_size":50}' \
          {{.API_HOST}} subflow.v1.SubflowService/ListBillingEvents

  update-context:
    desc: 'Update subscription context. Vars: ID, KEY, VALUE'
    cmds:
      - |
        grpcurl -plaintext -d '{"id":"{{.ID}}","updates":{"{{.KEY}}":"{{.VALUE}}"}}' \
          {{.API_HOST}} subflow.v1.SubflowService/UpdateSubscriptionContext
    requires: { vars: [ID, KEY, VALUE] }

  break-integration:
    desc: Stop mock-integration to demonstrate Temporal retry behavior
    cmds:
      - podman compose stop mock-integration

  fix-integration:
    desc: Restart mock-integration so retried hook activities drain
    cmds:
      - podman compose start mock-integration
```

- [ ] **Step 2: Smoke-test**

```bash
task --list 2>&1 | tail -25
```

Expected: all tasks listed.

- [ ] **Step 3: Commit**

```bash
git add Taskfile.yml
git commit -m "feat(taskfile): trial plan + list-billing-events + update-context + revised seed targets"
```

---

## Task 30: README + architecture + cheatsheet updates

**Files:**
- Rewrite: `README.md`
- Rewrite: `docs/architecture.md`
- Rewrite: `docs/temporal-cheatsheet.md`

- [ ] **Step 1: Rewrite `README.md`**

```markdown
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
```

- [ ] **Step 2: Rewrite `docs/architecture.md`**

```markdown
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
```

- [ ] **Step 3: Rewrite `docs/temporal-cheatsheet.md`**

```markdown
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
| Status read | Query `subscription.status` | `internal/workflow/handlers.go` AsStatus |
| List/filter subscriptions | `client.ListWorkflow` + custom search attributes | `internal/server/subscriptions.go` |
| Per-user limit | `client.CountWorkflow` | `internal/server/subscriptions.go` countActiveForUser |
| Charge | Activity `ChargePayment` with retry policy | `internal/activity/payment.go` |
| Billing history | Activity `RecordBillingEvent` writing to `billing_events` | `internal/activity/billing.go` + `internal/billing/mongo_store.go` |
| Lifecycle hooks | 10 activities dispatching to gRPC integration | `internal/activity/hooks.go` |
| Dunning loop | `workflow.Sleep` + `workflow.UpsertSearchAttributes` | `internal/workflow/dunning.go` |
| Idempotency token | `<workflowID>:<runID>:<purpose>` | `(*Subscription).activityRef` |

## Custom search attributes

| Name | Type | Set at start? | Upserted by workflow? |
|---|---|---|---|
| `SubflowUserId` | Keyword | Yes (`StartWorkflowOptions`) | No (immutable; carried by CAN) |
| `SubflowPlanCode` | Keyword | Yes | No (immutable; carried by CAN) |
| `SubflowPhase` | Keyword | Yes (initial) | Yes (`transitionTo` on every state change) |
| `SubflowPeriodEnd` | Datetime | Yes | Yes (`ContinueIntoNextPeriod` updates before CAN) |
| `SubflowTrialEnd` | Datetime | Conditionally (when `TrialDuration > 0`) | Set in `Trial()`; left in place after trial |

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
3. **Send a query from the UI**: Workflow → Query → `subscription.status`. Returns the live snapshot.
4. **Send a signal from the UI**: Workflow → Send Signal → `subscription.cancel`. Watch the workflow honor end-of-period.
5. **Run `temporal operator search-attribute list`** to confirm `SubflowUserId/PlanCode/Phase/PeriodEnd/TrialEnd` are registered.

## Why Continue-As-New per renewal?

Each billing period is its own discrete run with bounded history (~20 events). The workflow ID never changes; signals and queries continue to address the latest run. Keeps history footprint identical for a 1-year and a 50-year subscription, and makes each period visible as one row in the UI. CAN auto-carries search attributes, so the next run inherits all filtering visibility without re-upserting (except `SubflowPeriodEnd`, which changes per period).
```

- [ ] **Step 4: Commit**

```bash
git add README.md docs/architecture.md docs/temporal-cheatsheet.md
git commit -m "docs: README + architecture + cheatsheet for Temporal-native redesign"
```

---

## Task 31: Manual end-to-end validation

**Files:** none (operational walkthrough)

- [ ] **Step 1: Bring up the stack**

```bash
cd /Users/dmartavoi/Projects/subflow
API_HOST_PORT=50053 podman compose up -d --build
sleep 30
podman compose ps
```

Expected: 5 services up. `mongo` and `temporal` healthy.

If the API container fails because the search attribute registration failed, check the temporal logs (`podman compose logs temporal`). The dev server may need `--dynamic-config-value system.forceSearchAttributesCacheRefreshOnRead=false` added to its command. If so, edit `compose.yml`, recreate, and retry.

- [ ] **Step 2: Sanity — list services + custom SAs**

```bash
grpcurl -plaintext localhost:50053 list
podman exec subflow-temporal-1 /usr/local/bin/temporal operator search-attribute list --namespace default | grep Subflow
```

Expected: 5 `Subflow*` attributes listed.

- [ ] **Step 3: Seed two plans**

```bash
task seed-plan-monthly
task seed-plan-trial
task list-plans
```

Note both plan IDs.

- [ ] **Step 4: Non-trial subscription happy path**

```bash
task create-subscription USER=alice PLAN_ID=<monthly-basic-id>
# Response: phase=active, immediate.

# In Web UI: workflow `subscription:<id>` should be running, period_end ~30s out.
# Wait 35s for first renewal:
sleep 35
task get-subscription ID=<sub-id>
# Expected: renewal_count=1, period advanced, total_charged_cents = 1998.

task list-billing-events SUB_ID=<sub-id>
# Expected: 2 events (activation + renewal), type=charged each.
```

- [ ] **Step 5: Trial path**

```bash
task create-subscription USER=bob PLAN_ID=<trial-plan-id>
# Response: phase=trialing, immediate (no charge yet).

# Wait for trial to elapse (~30s) + first paid period to settle:
sleep 40
task get-subscription ID=<trial-sub-id>
# Expected: phase=active, last_charged_at populated.

task list-billing-events SUB_ID=<trial-sub-id>
# Expected: 1 event (the post-trial charge).
```

- [ ] **Step 6: Per-user limit**

```bash
task create-subscription USER=alice PLAN_ID=<monthly-basic-id>
# Expected: FailedPrecondition — Alice already has 1 active monthly-basic (per_user_limit=1).
```

- [ ] **Step 7: Dunning demo**

Stop mock-integration and let a renewal fail. Restart and watch recovery.

```bash
# Note: hook activities will retry when mock-integration is down. To demo
# the *charge* path failing, set PAYMENT_TRANSIENT_RATE high. Edit
# compose.yml to set environment: PAYMENT_TRANSIENT_RATE: "1.0" on the worker,
# then `podman compose up -d --build subflow-worker`.

# Watch the workflow enter past_due in the Web UI, retry through dunning,
# and either recover (if rate drops) or deactivate.
```

- [ ] **Step 8: Cancel + deactivate**

```bash
task cancel-subscription ID=<sub-id>
# Wait for period_end:
sleep 35
task get-subscription ID=<sub-id>
# Expected: phase=deactivated.
```

- [ ] **Step 9: Visibility-driven listing**

```bash
task list-subscriptions PHASE=active
task list-subscriptions USER=alice
task list-subscriptions PHASE=deactivated
```

Expected: filters work; no Mongo `subscriptions_view` collection exists.

```bash
podman exec subflow-mongo-1 mongosh --quiet --eval 'db.getSiblingDB("subflow").getCollectionNames()'
# Should show ["plans", "billing_events"] — NOT "subscriptions_view".
```

- [ ] **Step 10: Update subscription context**

```bash
task update-context ID=<sub-id> KEY=card_id VALUE=card_002
task get-subscription ID=<sub-id>
# Expected: context.card_id = "card_002".
```

- [ ] **Step 11: Tear down**

```bash
podman compose down
```

- [ ] **Step 12: Final commit if any tweaks were needed during validation**

If validation surfaced rough edges (broken healthcheck, missing log, wrong default), fix and commit. Otherwise skip.

```bash
git status
# Stage and commit any fixes.
```

---

## Self-review

### Spec coverage matrix

| Spec section | Implementing task(s) |
|---|---|
| §1 Why redesign | T1 (cleanup) — removes the projection |
| §2 Goals | All tasks; especially T22 (tests for lifecycle) |
| §3 Out of scope | Documented in T30 README "Roadmap" |
| §4 System architecture | T26 (worker), T27 (api), T28 (mock); diagram in T30 |
| §5 Lifecycle state machine | T16 (trial), T17 (activation), T18 (renewal), T19 (dunning), T20 (cancel/deactivate), T21 (Run wiring), verified by T22 |
| §6 Plan model | T3 (domain), T4 (store), T23 (server validation) |
| §7 Subscription entity | T12 (struct/ctor/wrapper), T13 (helpers), T14 (hooks), T15 (handlers), T16-T21 (per-aspect methods) |
| §8 Workflow primitives (signals/queries/updates/SAs) | T6 (SA bootstrapper), T12 (signal constants), T17 (Activate update), T24 (Visibility queries) |
| §9 Hooks integration contract | T2 (proto), T10 (client), T11 (activities), T28 (mock implements) |
| §10 Billing events store | T5 (package), T9 (activity), T13 (workflow integration), T25 (server endpoint) |
| §11 gRPC API surface | T2 (proto), T23-T25 (handlers) |
| §12 Persistence layout | T4 (plans), T5 (billing_events); T1 deletes the projection |
| §13 Error handling and retries | T7 (policies), T19 (dunning), T11 (hook error mapping) |
| §14 Testing strategy | T3 (domain tests), T22 (workflow tests) |
| §15 Migration plan | T1 (deletions); all subsequent tasks add replacements |

All sections covered.

### Placeholder scan

No `TBD`, `TODO`, "implement later", or "similar to task N". Every code block is complete and copy-paste-ready. The one ambiguity (`explicitlyZero` helper in T23) is documented inline with rationale rather than left as a TODO.

### Type / name consistency

- `Phase` constants (`PhaseTrialing`, `PhaseActive`, `PhasePastDue`, `PhaseCanceled`, `PhaseDeactivated`) — defined once in T12 (subscription.go), used in T13/T14/T15/T16/T17/T18/T19/T20.
- Signal / query / update name constants (`SignalCancelSubscription`, `SignalUpdateContext`, `QuerySubscriptionStatus`, `UpdateActivate`) — defined once in T12 (signals.go), referenced in T15 (handlers), T17 (activation), T20 (cancellation), T24 (server), T22 (tests).
- Activity registration names (`"ChargePayment"`, `"RecordBillingEvent"`, `"OnTrialStarted"` … 10 hook names) — registered in T26 (worker main), referenced via the same string literals in T13 (charge helper), T14 (hookActivityName), T22 (test mocks).
- `plan.Snapshot` fields (`Cadence`, `PriceCents`, `Currency`, `IntegrationEndpoint`, `EnabledHooks`, …) — defined in T3, referenced in T13/T14/T16/T17/T19/T24.
- Search attribute names (`AttrUserId`, `AttrPlanCode`, `AttrPhase`, `AttrPeriodEnd`, `AttrTrialEnd`) — defined in T6 (search_attributes.go), referenced in T13 (transitionTo), T20 (ContinueIntoNextPeriod), T24 (server query builders), T27 (api startup).
- `billing.Event` fields (`ID`, `SubscriptionID`, `UserID`, `PlanCode`, `Type`, `AmountCents`, …) — defined in T5, populated in T13, queried in T5's `MongoStore.List`, proto-mapped in T25.
- 10 hook names — defined in T3 (`plan.ValidHookNames`), validated in T23, mapped to activity names in T14 (`hookActivityName`), implemented in T11, dispatched in T10, mocked in T28.

All consistent.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-12-subflow-redesign-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
