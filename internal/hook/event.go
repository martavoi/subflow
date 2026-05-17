package hook

import "time"

// Event is the canonical integration-event payload published to integrators.
// Integration event (DDD-speak): published outward; the subscription workflow
// never reads its own emissions to drive internal state. See ADR 0002, 0007.
type Event struct {
	Type           Type
	SubscriptionID string
	UserID         string
	PlanCode       string
	RenewalCount   int
	OccurredAt time.Time

	// Context is the integrator-mutable key/value bag forwarded with every
	// hook (see CONTEXT.md glossary). Distinct from context.Context.
	Context map[string]string

	// Payload is the category-specific variant. Required; never nil for a
	// well-formed event.
	Payload Payload
}

// Payload is the category-specific variant carried on an Event. Sealed via
// the private isPayload marker — only payload types declared in this package
// satisfy it. New categories add a struct + isPayload().
type Payload interface{ isPayload() }

// LifecyclePayload carries the subscription's current phase and period
// boundaries — populated for hooks that report a state transition.
type LifecyclePayload struct {
	Phase       string
	PeriodStart time.Time
	PeriodEnd   time.Time
}

func (LifecyclePayload) isPayload() {}

// PaymentPayload carries charge-specific data — populated for hooks that
// report a payment outcome.
type PaymentPayload struct {
	DunningAttempt int
	AmountCents    int64
	Currency       string
	TransactionID  string
	FailureReason  string
}

func (PaymentPayload) isPayload() {}
