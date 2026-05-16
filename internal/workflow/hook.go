package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// activityNameForHook maps a hook.Type to the registered Temporal activity
// name. This mapping lives here (not in the hook package) because activity
// registration names are a workflow concern, not a domain concern.
func activityNameForHook(h hook.Type) string {
	switch h {
	case hook.TrialStarted:
		return "OnTrialStarted"
	case hook.TrialWillEnd:
		return "OnTrialWillEnd"
	case hook.Activated:
		return "OnActivated"
	case hook.Renewed:
		return "OnRenewed"
	case hook.PastDue:
		return "OnPastDue"
	case hook.Recovered:
		return "OnRecovered"
	case hook.Canceled:
		return "OnCanceled"
	case hook.Deactivated:
		return "OnDeactivated"
	case hook.PaymentOK:
		return "OnPaymentSucceeded"
	case hook.PaymentFailed:
		return "OnPaymentFailed"
	default:
		// RenewalUpcoming and any future hooks not yet wired to an activity.
		return ""
	}
}

// isEnabled reports whether the plan opted into this hook.
func isEnabled(h hook.Type, enabled []hook.Type) bool {
	for _, e := range enabled {
		if e == h {
			return true
		}
	}
	return false
}

// FireLifecycleHook dispatches a subscription-level hook. No-op if the plan
// has no integration endpoint, didn't opt into this hook, or the hook has no
// registered activity yet.
func (s *Subscription) FireLifecycleHook(ctx workflow.Context, h hook.Type) error {
	actName := activityNameForHook(h)
	if s.Plan.IntegrationEndpoint == "" || actName == "" || !isEnabled(h, s.Plan.EnabledHooks) {
		return nil
	}
	ref := s.idempotencyKey("hook:" + string(h))
	in := activity.LifecycleHookInput{
		Reference:           ref,
		IntegrationEndpoint: s.Plan.IntegrationEndpoint,
		HookName:            string(h),
		SubscriptionID:      s.SubscriptionID,
		UserID:              s.UserID,
		PlanCode:            s.PlanCode,
		Phase:               string(s.Phase),
		RenewalCount:        s.RenewalCount,
		PeriodStart:         s.Period.Start,
		PeriodEnd:           s.Period.End,
		EventTime:           workflow.Now(ctx),
		Context:             map[string]string(s.Context),
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.HookRetry,
	})
	return workflow.ExecuteActivity(opts, actName, in).Get(ctx, nil)
}

// FirePaymentHook dispatches a payment-level hook. Same gating + retry policy
// as FireLifecycleHook; different payload.
func (s *Subscription) FirePaymentHook(ctx workflow.Context, h hook.Type, dunningAttempt int, transactionID, failureReason string) error {
	actName := activityNameForHook(h)
	if s.Plan.IntegrationEndpoint == "" || actName == "" || !isEnabled(h, s.Plan.EnabledHooks) {
		return nil
	}
	ref := s.idempotencyKey(fmt.Sprintf("hook:%s:%d", string(h), dunningAttempt))
	in := activity.PaymentHookInput{
		Reference:           ref,
		IntegrationEndpoint: s.Plan.IntegrationEndpoint,
		HookName:            string(h),
		SubscriptionID:      s.SubscriptionID,
		UserID:              s.UserID,
		PlanCode:            s.PlanCode,
		RenewalCount:        s.RenewalCount,
		DunningAttempt:      dunningAttempt,
		AmountCents:         s.Plan.PriceCents,
		Currency:            s.Plan.Currency,
		TransactionID:       transactionID,
		FailureReason:       failureReason,
		EventTime:           workflow.Now(ctx),
		Context:             map[string]string(s.Context),
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.HookRetry,
	})
	return workflow.ExecuteActivity(opts, actName, in).Get(ctx, nil)
}
