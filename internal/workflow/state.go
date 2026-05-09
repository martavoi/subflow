package workflow

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
)

// SubscriptionStatus is the snapshot returned by the QuerySubscriptionStatus
// query handler. Mirrors the read-model fields the API needs.
type SubscriptionStatus struct {
	Phase           string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool
}

// SubscriptionState is the in-memory state mutated during a single workflow
// run. It is recreated from input + replay on every run; not persisted.
type SubscriptionState struct {
	Input subscription.SubscriptionInput
	Phase string
}

// AsStatus returns the queryable snapshot.
func (s *SubscriptionState) AsStatus() (SubscriptionStatus, error) {
	return SubscriptionStatus{
		Phase:           s.Phase,
		PeriodStart:     s.Input.PeriodStart,
		PeriodEnd:       s.Input.PeriodEnd,
		RenewalCount:    s.Input.RenewalCount,
		Context:         s.Input.Context.Clone(),
		CancelRequested: s.Input.CancelRequested,
	}, nil
}
