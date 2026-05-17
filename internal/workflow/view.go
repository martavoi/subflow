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
