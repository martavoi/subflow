package activity

import (
	"context"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/store"
)

const (
	PhasePending     = "pending"
	PhaseActive      = "active"
	PhaseCancelling  = "cancelling"
	PhaseDeactivated = "deactivated"
)

type ProjectionUpdate struct {
	SubscriptionID  string
	UserID          string
	PlanID          string
	Phase           string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool
}

type ProjectionActivities struct {
	Repo *store.SubscriptionProjectionRepository
}

func (a *ProjectionActivities) UpdateSubscriptionProjection(ctx context.Context, u ProjectionUpdate) error {
	return a.Repo.Upsert(ctx, store.SubscriptionView{
		ID:              u.SubscriptionID,
		UserID:          u.UserID,
		PlanID:          u.PlanID,
		Phase:           u.Phase,
		PeriodStart:     u.PeriodStart,
		PeriodEnd:       u.PeriodEnd,
		RenewalCount:    u.RenewalCount,
		Context:         u.Context,
		CancelRequested: u.CancelRequested,
	})
}
