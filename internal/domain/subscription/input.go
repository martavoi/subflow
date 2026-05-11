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
