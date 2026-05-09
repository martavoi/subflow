package activity

import (
	"context"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/eventbus"
)

const (
	EventTypeActivate   = "subscription.activate"
	EventTypeRenew      = "subscription.renew"
	EventTypeDeactivate = "subscription.deactivate"
)

type PublishEventInput struct {
	Reference      string
	EventType      string
	SubscriptionID string
	UserID         string
	PlanID         string
	PlanCode       string
	PeriodStart    time.Time
	PeriodEnd      time.Time
	RenewalCount   int
	Context        subscription.Context
}

type EventActivities struct {
	Publisher eventbus.Publisher
	Now       func() time.Time // injectable for tests; defaults to time.Now in main
}

func (a *EventActivities) PublishSubscriptionEvent(ctx context.Context, in PublishEventInput) error {
	now := a.Now
	if now == nil {
		now = time.Now
	}
	ev := eventbus.Event{
		Reference:      in.Reference,
		Type:           in.EventType,
		SubscriptionID: in.SubscriptionID,
		UserID:         in.UserID,
		PlanID:         in.PlanID,
		PlanCode:       in.PlanCode,
		PeriodStart:    in.PeriodStart,
		PeriodEnd:      in.PeriodEnd,
		RenewalCount:   in.RenewalCount,
		Context:        in.Context,
		OccurredAt:     now().UTC(),
	}
	return a.Publisher.Publish(ctx, ev)
}
