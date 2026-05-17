package hook

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event is the canonical integration-event payload published to integrators.
// Integration event (DDD-speak): published outward; the subscription workflow
// never reads its own emissions to drive internal state. See ADR 0002, 0007.
type Event struct {
	Type           Type
	SubscriptionID string
	UserID         string
	PlanCode       string
	RenewalCount   int
	OccurredAt     time.Time

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

// payloadKind is the discriminator tag stored alongside the payload in JSON.
type payloadKind string

const (
	payloadKindLifecycle payloadKind = "lifecycle"
	payloadKindPayment   payloadKind = "payment"
)

// eventJSON is the wire shape used for JSON serialisation of Event.
type eventJSON struct {
	Type           Type              `json:"type"`
	SubscriptionID string            `json:"subscription_id"`
	UserID         string            `json:"user_id"`
	PlanCode       string            `json:"plan_code"`
	RenewalCount   int               `json:"renewal_count"`
	OccurredAt     time.Time         `json:"occurred_at"`
	Context        map[string]string `json:"context,omitempty"`
	PayloadKind    payloadKind       `json:"payload_kind"`
	PayloadData    json.RawMessage   `json:"payload_data"`
}

// MarshalJSON encodes an Event as JSON, tagging the payload with its kind so
// UnmarshalJSON can reconstruct the correct concrete type.
func (e Event) MarshalJSON() ([]byte, error) {
	var kind payloadKind
	switch e.Payload.(type) {
	case LifecyclePayload:
		kind = payloadKindLifecycle
	case PaymentPayload:
		kind = payloadKindPayment
	default:
		return nil, fmt.Errorf("hook.Event.MarshalJSON: unknown payload type %T", e.Payload)
	}

	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return nil, err
	}

	return json.Marshal(eventJSON{
		Type:           e.Type,
		SubscriptionID: e.SubscriptionID,
		UserID:         e.UserID,
		PlanCode:       e.PlanCode,
		RenewalCount:   e.RenewalCount,
		OccurredAt:     e.OccurredAt,
		Context:        e.Context,
		PayloadKind:    kind,
		PayloadData:    raw,
	})
}

// UnmarshalJSON decodes a JSON Event, using the payload_kind discriminator to
// instantiate the correct Payload concrete type.
func (e *Event) UnmarshalJSON(data []byte) error {
	var wire eventJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	e.Type = wire.Type
	e.SubscriptionID = wire.SubscriptionID
	e.UserID = wire.UserID
	e.PlanCode = wire.PlanCode
	e.RenewalCount = wire.RenewalCount
	e.OccurredAt = wire.OccurredAt
	e.Context = wire.Context

	switch wire.PayloadKind {
	case payloadKindLifecycle:
		var p LifecyclePayload
		if err := json.Unmarshal(wire.PayloadData, &p); err != nil {
			return err
		}
		e.Payload = p
	case payloadKindPayment:
		var p PaymentPayload
		if err := json.Unmarshal(wire.PayloadData, &p); err != nil {
			return err
		}
		e.Payload = p
	default:
		return fmt.Errorf("hook.Event.UnmarshalJSON: unknown payload_kind %q", wire.PayloadKind)
	}
	return nil
}
