package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)

// ContinueIntoNextPeriod CANs the workflow with the next period's input.
// Upserts the SubflowPeriodEnd search attribute first because the new period
// boundary should be visible immediately for "expiring soon" queries.
func (s *Subscription) ContinueIntoNextPeriod(ctx workflow.Context) error {
	next := subscription.NextBillingPeriod(s.toInput())
	_ = workflow.UpsertSearchAttributes(ctx, map[string]any{
		subflowtemporal.AttrPeriodEnd: next.PeriodEnd,
	})
	return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, next)
}

// toInput serializes the entity's relevant state back into a
// SubscriptionInput for CAN. The next run reconstructs from this.
func (s *Subscription) toInput() subscription.SubscriptionInput {
	return subscription.SubscriptionInput{
		SubscriptionID:  s.ID,
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
