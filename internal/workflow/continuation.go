package workflow

import (
	"github.com/google/uuid"
	"github.com/martavoi/subflow/internal/domain/subscription"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)

// ContinueIntoNextPeriod CANs the workflow with the next period's input.
// Generates a fresh IntervalID via SideEffect (UUID is non-deterministic, so
// it must be recorded in history) and upserts the SubflowPeriodEnd search
// attribute so the new period boundary is visible immediately for
// "expiring soon" queries.
func (s *Subscription) ContinueIntoNextPeriod(ctx workflow.Context) error {
	var nextIntervalID string
	_ = workflow.SideEffect(ctx, func(workflow.Context) any {
		return uuid.NewString()
	}).Get(&nextIntervalID)

	next := subscription.NextBillingPeriod(s.toInput(), nextIntervalID)
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		subflowtemporal.KeyPeriodEnd.ValueSet(next.PeriodEnd),
	)
	return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, next)
}

// toInput serializes the entity's relevant state back into a
// SubscriptionInput for CAN. The next run reconstructs from this.
// IntervalID is left zero — the caller supplies a fresh one.
func (s *Subscription) toInput() subscription.SubscriptionInput {
	return subscription.SubscriptionInput{
		SubscriptionID:  s.SubscriptionID,
		UserID:          s.UserID,
		PlanID:          s.PlanID,
		Plan:            s.Plan,
		PeriodStart:     s.Period.Start,
		PeriodEnd:       s.Period.End,
		Context:         s.Context.Clone(),
		RenewalCount:    s.RenewalCount,
		CancelRequested: s.CancelRequested,
	}
}
