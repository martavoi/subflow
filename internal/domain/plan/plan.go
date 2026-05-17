package plan

import (
	"time"

	"github.com/martavoi/subflow/internal/hook"
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
	TrialDuration          time.Duration
	TrialEndNoticeBefore   time.Duration
	RenewalUpcomingBefore  time.Duration
	DunningMaxAttempts     int
	DunningRetryBackoff    time.Duration
	IntegrationEndpoint    string
	EnabledHooks           []hook.Type
	CreatedAt              time.Time
}

