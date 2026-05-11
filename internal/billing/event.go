package billing

import "time"

// Type values for Event.Type.
const (
	TypeCharged  = "charged"
	TypeFailed   = "failed"
	TypeRefunded = "refunded" // reserved for roadmap
)

// Event is an immutable billing record. One Event = one charge attempt
// (success or failure) on a subscription. Append-only — never updated,
// never deleted.
type Event struct {
	ID             string
	SubscriptionID string
	UserID         string
	PlanCode       string
	Type           string
	AmountCents    int64
	Currency       string
	PeriodStart    time.Time
	PeriodEnd      time.Time
	RenewalCount   int
	DunningAttempt int
	TransactionID  string
	FailureReason  string
	OccurredAt     time.Time
	Reference      string
}

// ListQuery filters and paginates Event lookups.
type ListQuery struct {
	SubscriptionID string
	UserID         string
	TypeFilter     string
	PageSize       int
	PageCursor     string
}

const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)
