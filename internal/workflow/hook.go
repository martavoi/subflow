package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/plan"
	"go.temporal.io/sdk/workflow"
)

// Hook is a typed identifier for a lifecycle event the integration can
// subscribe to. Each Hook bundles its wire name (the string used over the
// network and in plan configuration) with the registered Temporal activity
// name. Co-locating both eliminates name-mapping switches.
type Hook struct {
	Name         string
	ActivityName string
}

// String makes Hook printable via the wire name.
func (h Hook) String() string { return h.Name }

// The 10 hooks subflow supports.
var (
	HookTrialStarted  = Hook{Name: "subscription.trial_started", ActivityName: "OnTrialStarted"}
	HookTrialWillEnd  = Hook{Name: "subscription.trial_will_end", ActivityName: "OnTrialWillEnd"}
	HookActivated     = Hook{Name: "subscription.activated", ActivityName: "OnActivated"}
	HookRenewed       = Hook{Name: "subscription.renewed", ActivityName: "OnRenewed"}
	HookPastDue       = Hook{Name: "subscription.past_due", ActivityName: "OnPastDue"}
	HookRecovered     = Hook{Name: "subscription.recovered", ActivityName: "OnRecovered"}
	HookCanceled      = Hook{Name: "subscription.canceled", ActivityName: "OnCanceled"}
	HookDeactivated   = Hook{Name: "subscription.deactivated", ActivityName: "OnDeactivated"}
	HookPaymentOK     = Hook{Name: "payment.succeeded", ActivityName: "OnPaymentSucceeded"}
	HookPaymentFailed = Hook{Name: "payment.failed", ActivityName: "OnPaymentFailed"}
)

// AllHooks is the canonical list — used for plan validation and tests.
var AllHooks = []Hook{
	HookTrialStarted, HookTrialWillEnd,
	HookActivated, HookRenewed,
	HookPastDue, HookRecovered,
	HookCanceled, HookDeactivated,
	HookPaymentOK, HookPaymentFailed,
}

// HookByName returns the Hook for a wire name, or (Hook{}, false) if unknown.
func HookByName(name string) (Hook, bool) {
	for _, h := range AllHooks {
		if h.Name == name {
			return h, true
		}
	}
	return Hook{}, false
}

// isEnabled reports whether the plan opted into this hook. Reads the typed
// HookName slice from the plan snapshot directly — no string mapping.
func (h Hook) isEnabled(enabled []plan.HookName) bool {
	for _, n := range enabled {
		if string(n) == h.Name {
			return true
		}
	}
	return false
}

// FireLifecycleHook dispatches a subscription-level hook. No-op if the plan
// has no integration endpoint or didn't opt into this hook.
func (s *Subscription) FireLifecycleHook(ctx workflow.Context, h Hook) error {
	if s.Plan.IntegrationEndpoint == "" || !h.isEnabled(s.Plan.EnabledHooks) {
		return nil
	}
	ref := s.idempotencyKey("hook:" + h.Name)
	in := activity.LifecycleHookInput{
		Reference:           ref,
		IntegrationEndpoint: s.Plan.IntegrationEndpoint,
		HookName:            h.Name,
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
	return workflow.ExecuteActivity(opts, h.ActivityName, in).Get(ctx, nil)
}

// FirePaymentHook dispatches a payment-level hook. Same gating + retry policy
// as FireLifecycleHook; different payload.
func (s *Subscription) FirePaymentHook(ctx workflow.Context, h Hook, dunningAttempt int, transactionID, failureReason string) error {
	if s.Plan.IntegrationEndpoint == "" || !h.isEnabled(s.Plan.EnabledHooks) {
		return nil
	}
	ref := s.idempotencyKey(fmt.Sprintf("hook:%s:%d", h.Name, dunningAttempt))
	in := activity.PaymentHookInput{
		Reference:           ref,
		IntegrationEndpoint: s.Plan.IntegrationEndpoint,
		HookName:            h.Name,
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
	return workflow.ExecuteActivity(opts, h.ActivityName, in).Get(ctx, nil)
}
