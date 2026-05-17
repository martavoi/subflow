package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// isEnabled reports whether the plan opted into this hook.
func isEnabled(h hook.Type, enabled []hook.Type) bool {
	for _, e := range enabled {
		if e == h {
			return true
		}
	}
	return false
}

// fireLifecycle dispatches a subscription-level hook. No-op if the plan has no
// integration endpoint or didn't opt into this hook.
func (s *Subscription) fireLifecycle(ctx workflow.Context, h hook.Type) error {
	if s.Plan.IntegrationEndpoint == "" || !isEnabled(h, s.Plan.EnabledHooks) {
		return nil
	}
	in := DispatchHook{
		Endpoint:       s.Plan.IntegrationEndpoint,
		EventID:        s.idempotencyKey("hook:" + string(h)),
		Type:           h,
		SubscriptionID: s.SubscriptionID,
		UserID:         s.UserID,
		PlanCode:       s.PlanCode,
		RenewalCount:   s.RenewalCount,
		EventTime:      workflow.Now(ctx),
		Context:        map[string]string(s.Context),
		Lifecycle: &LifecycleData{
			Phase:       string(s.Phase),
			PeriodStart: s.Period.Start,
			PeriodEnd:   s.Period.End,
		},
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         HookRetry,
	})
	return workflow.ExecuteActivity(opts, "DispatchHook", in).Get(ctx, nil)
}

// firePayment dispatches a payment-level hook. Same gating + retry policy as
// fireLifecycle; different payload variant.
func (s *Subscription) firePayment(ctx workflow.Context, h hook.Type, dunningAttempt int, transactionID, failureReason string) error {
	if s.Plan.IntegrationEndpoint == "" || !isEnabled(h, s.Plan.EnabledHooks) {
		return nil
	}
	in := DispatchHook{
		Endpoint:       s.Plan.IntegrationEndpoint,
		EventID:        s.idempotencyKey(fmt.Sprintf("hook:%s:%d", string(h), dunningAttempt)),
		Type:           h,
		SubscriptionID: s.SubscriptionID,
		UserID:         s.UserID,
		PlanCode:       s.PlanCode,
		RenewalCount:   s.RenewalCount,
		EventTime:      workflow.Now(ctx),
		Context:        map[string]string(s.Context),
		Payment: &PaymentData{
			DunningAttempt: dunningAttempt,
			AmountCents:    s.Plan.PriceCents,
			Currency:       s.Plan.Currency,
			TransactionID:  transactionID,
			FailureReason:  failureReason,
		},
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         HookRetry,
	})
	return workflow.ExecuteActivity(opts, "DispatchHook", in).Get(ctx, nil)
}
