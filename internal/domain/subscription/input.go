package subscription

import "time"

// SubscriptionInput is the workflow input. Carried across Continue-As-New
// so the next run can resume cleanly.
type SubscriptionInput struct {
	SubscriptionID  string
	UserID          string
	PlanID          string
	PlanCode        string
	BillingInterval time.Duration
	IntegrationHost string
	PriceCents      int64
	PeriodStart     time.Time
	PeriodEnd       time.Time
	Context         Context
	RenewalCount    int
	CancelRequested bool
}

// IsActivation reports whether this run represents the first billing period
// of the subscription.
func (in SubscriptionInput) IsActivation() bool {
	return in.RenewalCount == 0
}
