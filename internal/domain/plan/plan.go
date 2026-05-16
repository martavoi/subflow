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
	TrialDuration        time.Duration
	TrialEndNoticeBefore time.Duration
	DunningMaxAttempts   int
	DunningRetryBackoff  time.Duration
	IntegrationEndpoint  string
	EnabledHooks         []hook.Type
	CreatedAt            time.Time
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
	EnabledHooks         []hook.Type
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
		EnabledHooks:         append([]hook.Type(nil), p.EnabledHooks...),
	}
}
