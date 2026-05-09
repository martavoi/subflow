package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// activityRef constructs an idempotency token for an activity call.
// Stable across retries within a run, unique across runs (run ID changes).
func activityRef(ctx workflow.Context, suffix string) string {
	info := workflow.GetInfo(ctx)
	return fmt.Sprintf("%s:%s:%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, suffix)
}

// StartBillingPeriod dispatches to activation or renewal based on whether this
// is the first billing period.
func StartBillingPeriod(ctx workflow.Context, state *SubscriptionState) error {
	if state.Input.IsActivation() {
		return ActivateSubscription(ctx, state)
	}
	return RenewSubscription(ctx, state)
}

// ActivateSubscription runs the period-start activities for the very first
// billing period: charge → publish → notify integration → project.
func ActivateSubscription(ctx workflow.Context, state *SubscriptionState) error {
	state.Phase = activity.PhasePending
	if err := updateProjection(ctx, state); err != nil {
		return err
	}
	if err := chargeAndPublish(ctx, state, activity.EventTypeActivate); err != nil {
		return err
	}
	if err := notifyIntegrationAndUpdateContext(ctx, state, activity.EventTypeActivate); err != nil {
		return err
	}
	state.Phase = activity.PhaseActive
	return updateProjection(ctx, state)
}

// RenewSubscription runs the period-start activities for a renewal period.
func RenewSubscription(ctx workflow.Context, state *SubscriptionState) error {
	if err := chargeAndPublish(ctx, state, activity.EventTypeRenew); err != nil {
		return err
	}
	if err := notifyIntegrationAndUpdateContext(ctx, state, activity.EventTypeRenew); err != nil {
		return err
	}
	state.Phase = activity.PhaseActive
	return updateProjection(ctx, state)
}

// DeactivateSubscription publishes the deactivation event, notifies the
// integration service, and writes the terminal projection.
func DeactivateSubscription(ctx workflow.Context, state *SubscriptionState) error {
	state.Phase = activity.PhaseCancelling
	if err := updateProjection(ctx, state); err != nil {
		return err
	}
	if err := publishLifecycleEvent(ctx, state, activity.EventTypeDeactivate); err != nil {
		return err
	}
	if err := notifyIntegrationAndUpdateContext(ctx, state, activity.EventTypeDeactivate); err != nil {
		return err
	}
	state.Phase = activity.PhaseDeactivated
	return updateProjection(ctx, state)
}

// AwaitPeriodEndOrCancellation parks the workflow until the period timer
// fires or a cancel signal is received. End-of-period semantics: if a cancel
// arrives early, sleep the remainder of the period before returning.
//
// Returns true if the subscription was cancelled.
func AwaitPeriodEndOrCancellation(ctx workflow.Context, state *SubscriptionState) bool {
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancelSubscription)
	cancelled := state.Input.CancelRequested

	now := workflow.Now(ctx)
	if state.Input.PeriodEnd.After(now) {
		timer := workflow.NewTimer(ctx, state.Input.PeriodEnd.Sub(now))
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(timer, func(workflow.Future) {})
		sel.AddReceive(cancelCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			cancelled = true
			state.Input.CancelRequested = true
			state.Phase = activity.PhaseCancelling
			_ = updateProjection(ctx, state)
		})
		sel.Select(ctx)
	}

	// Honor end-of-period: if cancel arrived early, sleep the remainder.
	if cancelled {
		remaining := state.Input.PeriodEnd.Sub(workflow.Now(ctx))
		if remaining > 0 {
			_ = workflow.Sleep(ctx, remaining)
		}
	}
	return cancelled
}

// ContinueIntoNextPeriod restarts the workflow as a new run for the next
// billing period (Continue-As-New per renewal).
func ContinueIntoNextPeriod(ctx workflow.Context, state *SubscriptionState) error {
	return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow,
		subscription.NextBillingPeriod(state.Input))
}

// --- helpers below — kept private intentionally because they are pure
// orchestration glue, not domain verbs. Each domain verb above remains the
// public, named, testable surface.

func chargeAndPublish(ctx workflow.Context, state *SubscriptionState, eventType string) error {
	chargeIn := activity.ChargePaymentInput{
		Reference:  activityRef(ctx, "charge:"+eventType),
		UserID:     state.Input.UserID,
		PriceCents: state.Input.PriceCents,
	}
	chargeOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.PaymentRetry,
	})
	var chargeRes activity.ChargePaymentResult
	if err := workflow.ExecuteActivity(chargeOpts, "ChargePayment", chargeIn).Get(ctx, &chargeRes); err != nil {
		return err
	}
	return publishLifecycleEvent(ctx, state, eventType)
}

func publishLifecycleEvent(ctx workflow.Context, state *SubscriptionState, eventType string) error {
	pubIn := activity.PublishEventInput{
		Reference:      activityRef(ctx, "publish:"+eventType),
		EventType:      eventType,
		SubscriptionID: state.Input.SubscriptionID,
		UserID:         state.Input.UserID,
		PlanID:         state.Input.PlanID,
		PlanCode:       state.Input.PlanCode,
		PeriodStart:    state.Input.PeriodStart,
		PeriodEnd:      state.Input.PeriodEnd,
		RenewalCount:   state.Input.RenewalCount,
		Context:        state.Input.Context.Clone(),
	}
	pubOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy:         activity.EventPublishingRetry,
	})
	return workflow.ExecuteActivity(pubOpts, "PublishSubscriptionEvent", pubIn).Get(ctx, nil)
}

func notifyIntegrationAndUpdateContext(ctx workflow.Context, state *SubscriptionState, eventType string) error {
	notifyIn := activity.IntegrationCallInput{
		Reference:       activityRef(ctx, "integration:"+eventType),
		EventType:       eventType,
		UserID:          state.Input.UserID,
		PlanCode:        state.Input.PlanCode,
		IntegrationHost: state.Input.IntegrationHost,
		Context:         state.Input.Context.Clone(),
	}
	notifyOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.IntegrationCallRetry,
	})
	var notifyRes activity.IntegrationCallResult
	if err := workflow.ExecuteActivity(notifyOpts, "NotifyIntegrationService", notifyIn).Get(ctx, &notifyRes); err != nil {
		return err
	}
	if notifyRes.UpdatedContext != nil {
		state.Input.Context = notifyRes.UpdatedContext
	}
	return nil
}

func updateProjection(ctx workflow.Context, state *SubscriptionState) error {
	upd := activity.ProjectionUpdate{
		SubscriptionID:  state.Input.SubscriptionID,
		UserID:          state.Input.UserID,
		PlanID:          state.Input.PlanID,
		Phase:           state.Phase,
		PeriodStart:     state.Input.PeriodStart,
		PeriodEnd:       state.Input.PeriodEnd,
		RenewalCount:    state.Input.RenewalCount,
		Context:         state.Input.Context.Clone(),
		CancelRequested: state.Input.CancelRequested,
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         activity.EventPublishingRetry, // same gentle retry — projection should always eventually write
	})
	return workflow.ExecuteActivity(opts, "UpdateSubscriptionProjection", upd).Get(ctx, nil)
}
