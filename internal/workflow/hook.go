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

// emitLifecycle fires a lifecycle hook. Payload data is entirely derived
// from current subscription state — no extra arguments.
func (s *Subscription) emitLifecycle(ctx workflow.Context, t hook.Type) error {
	return s.dispatch(ctx, hook.Event{
		Type: t,
		Payload: hook.LifecyclePayload{
			Phase:       string(s.Phase),
			PeriodStart: s.Period.Start,
			PeriodEnd:   s.Period.End,
		},
	})
}

// emitPayment fires a payment hook. Caller supplies the full payload; no
// magic fill-in from subscription state.
func (s *Subscription) emitPayment(ctx workflow.Context, t hook.Type, p hook.PaymentPayload) error {
	return s.dispatch(ctx, hook.Event{Type: t, Payload: p})
}

// dispatch is the internal common-path for both emit variants: plan gating,
// common-field fill-in, idempotency key derivation, activity invocation.
func (s *Subscription) dispatch(ctx workflow.Context, e hook.Event) error {
	if s.Plan.IntegrationEndpoint == "" || !isEnabled(e.Type, s.Plan.EnabledHooks) {
		return nil
	}
	e.SubscriptionID = s.SubscriptionID
	e.UserID = s.UserID
	e.PlanCode = s.PlanCode
	e.RenewalCount = s.RenewalCount
	e.OccurredAt = workflow.Now(ctx)
	e.Context = map[string]string(s.Context)

	in := DispatchHook{
		Event:    e,
		Endpoint: s.Plan.IntegrationEndpoint,
		EventID:  s.hookID(e),
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         HookRetry,
	})
	return workflow.ExecuteActivity(opts, "DispatchHook", in).Get(ctx, nil)
}

// hookID derives the idempotency key. Payment payloads include DunningAttempt
// so retries on the same hook type across dunning attempts get distinct event
// IDs.
func (s *Subscription) hookID(e hook.Event) string {
	if p, ok := e.Payload.(hook.PaymentPayload); ok {
		return s.idempotencyKey(fmt.Sprintf("hook:%s:%d", e.Type, p.DunningAttempt))
	}
	return s.idempotencyKey("hook:" + string(e.Type))
}
