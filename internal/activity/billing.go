package activity

import (
	"context"

	"github.com/martavoi/subflow/internal/billing"
)

// BillingStore holds the EventStore dependency for billing-related activities.
type BillingStore struct {
	Events billing.EventStore
}

// RecordBillingEvent appends a single billing event. EventStore implementations
// must be idempotent on Event.Reference so retries don't double-write.
func (a *BillingStore) RecordBillingEvent(ctx context.Context, ev billing.Event) error {
	return a.Events.Append(ctx, ev)
}
