package activity

import (
	"context"

	"github.com/martavoi/subflow/internal/billing"
)

// BillingActivities holds the EventStore dependency for billing-related activities.
type BillingActivities struct {
	Events billing.EventStore
}

// RecordBillingEvent appends a single billing event. EventStore implementations
// must be idempotent on Event.Reference so retries don't double-write.
func (a *BillingActivities) RecordBillingEvent(ctx context.Context, ev billing.Event) error {
	return a.Events.Append(ctx, ev)
}
