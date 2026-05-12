package workflow

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
)

// Status is the snapshot returned by the QuerySubscriptionStatus query.
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
