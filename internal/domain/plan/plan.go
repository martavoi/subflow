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
// typed HookName slice the plan persists. Rejects any unknown name.
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
