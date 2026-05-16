package subscription

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
