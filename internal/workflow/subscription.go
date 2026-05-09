package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// UpdateActivate is the synchronous activation update sent by the API
// immediately after starting the workflow. The handler runs the activation
// activities (charge, publish, integration call, projection write) and
// returns the post-activation state to the caller. This makes
// CreateSubscription a true sync operation: the customer learns whether
// their card was charged before the gRPC call returns.
const UpdateActivate = "subscription.activate"

// ActivationResult is what the Activate update returns to the caller. On
// failure (e.g. card declined) the update returns an error and this struct
// is not populated.
type ActivationResult struct {
	Phase   string
	Context subscription.Context
}

// SubscriptionWorkflow is the durable subscription lifecycle. One workflow
// run per billing period; Continue-As-New advances to the next period until
// the subscription is cancelled.
//
// First-period (activation): the workflow blocks waiting for the Activate
// update. The update handler runs the activation activities synchronously
// inside the workflow context and returns the result to the API caller.
//
// Subsequent periods (renewals): the workflow runs renewal activities
// directly, since there is no synchronous customer waiting on a renewal.
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

	if in.IsActivation() {
		activated := false
		err := workflow.SetUpdateHandler(ctx, UpdateActivate,
			func(ctx workflow.Context) (ActivationResult, error) {
				if err := ActivateSubscription(ctx, state); err != nil {
					return ActivationResult{}, err
				}
				activated = true
				return ActivationResult{
					Phase:   state.Phase,
					Context: state.Input.Context.Clone(),
				}, nil
			},
		)
		if err != nil {
			return err
		}
		if err := workflow.Await(ctx, func() bool { return activated }); err != nil {
			return err
		}
	} else {
		if err := RenewSubscription(ctx, state); err != nil {
			return err
		}
	}

	cancelled := AwaitPeriodEndOrCancellation(ctx, state)
	if cancelled {
		return DeactivateSubscription(ctx, state)
	}

	return ContinueIntoNextPeriod(ctx, state)
}
