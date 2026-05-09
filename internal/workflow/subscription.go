package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// SubscriptionWorkflow is the durable subscription lifecycle. One workflow
// run per billing period; Continue-As-New advances to the next period until
// the subscription is cancelled.
//
// Workflow ID convention: "subscription:<SubscriptionID>" — addressable for
// signals and queries by the API layer.
func SubscriptionWorkflow(ctx workflow.Context, in subscription.SubscriptionInput) error {
	state := &SubscriptionState{
		Input: in,
		Phase: "starting",
	}

	if err := workflow.SetQueryHandler(ctx, QuerySubscriptionStatus, state.AsStatus); err != nil {
		return err
	}

	if err := StartBillingPeriod(ctx, state); err != nil {
		return err
	}

	cancelled := AwaitPeriodEndOrCancellation(ctx, state)
	if cancelled {
		return DeactivateSubscription(ctx, state)
	}

	return ContinueIntoNextPeriod(ctx, state)
}
