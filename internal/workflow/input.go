package workflow

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
)

// SubscriptionInput is the workflow input. Carried across Continue-As-New
// so the next run can resume cleanly.
//
// IntervalID identifies the current billing interval (trial, first paid period,
// each renewal). Rotated on every CAN; stable within a run. Combined with
// SubscriptionID it produces durable idempotency keys for charges and hooks
// without coupling to Temporal exec/run IDs.
type SubscriptionInput struct {
	SubscriptionID  string
	IntervalID      string
	UserID          string
	PlanID          string
	Plan            plan.Plan
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

// Context is the per-subscription mutable key-value bag exchanged with the
// integration service across each lifecycle action. Mirrors the contract
// of the upstream subscription service.
type Context map[string]string

// Clone returns an independent copy of the context (workflows should never
// share map references between runs).
func (c Context) Clone() Context {
	out := make(Context, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}

// NextBillingPeriod returns a SubscriptionInput for the period immediately
// following `current`. Pure function (no time.Now, no randomness — the caller
// supplies the fresh IntervalID since UUID generation is non-deterministic
// and belongs at the workflow boundary via workflow.SideEffect).
func NextBillingPeriod(current SubscriptionInput, nextIntervalID string) SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  current.SubscriptionID,
		IntervalID:      nextIntervalID,
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
