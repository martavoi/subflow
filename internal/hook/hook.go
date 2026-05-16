// Package hook owns the Hook taxonomy as a single source of truth.
// Both internal/domain/plan and internal/workflow consume from it.
package hook

import "fmt"

// Type is the wire name of a lifecycle hook. String-backed so it round-trips
// cleanly through BSON and JSON without any custom (de)serialisation.
type Type string

// The 11 hooks Subflow supports. The string value is the canonical wire name
// used in plan configuration, BSON storage, and integration event payloads.
const (
	TrialStarted    Type = "subscription.trial_started"
	TrialWillEnd    Type = "subscription.trial_will_end"
	RenewalUpcoming Type = "subscription.renewal_upcoming"
	Activated       Type = "subscription.activated"
	Renewed         Type = "subscription.renewed"
	PastDue         Type = "subscription.past_due"
	Recovered       Type = "subscription.recovered"
	Canceled        Type = "subscription.canceled"
	Deactivated     Type = "subscription.deactivated"
	PaymentOK       Type = "payment.succeeded"
	PaymentFailed   Type = "payment.failed"
)

// All is the canonical list of every supported hook type. Used for validation
// and for ranging over all hooks in tests.
var All = []Type{
	TrialStarted,
	TrialWillEnd,
	RenewalUpcoming,
	Activated,
	Renewed,
	PastDue,
	Recovered,
	Canceled,
	Deactivated,
	PaymentOK,
	PaymentFailed,
}

// Parse converts a raw wire-name string into the typed Hook constant.
// Returns an error if the name is not in All.
func Parse(s string) (Type, error) {
	for _, h := range All {
		if string(h) == s {
			return h, nil
		}
	}
	return "", fmt.Errorf("unknown hook name: %q (allowed: %v)", s, All)
}
