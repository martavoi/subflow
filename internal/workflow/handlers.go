package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// AsStatus is the query handler — returns the current entity snapshot.
// Pure read; no mutation. Bound to s for use as a method-value handler.
func (s *Subscription) AsStatus() (Status, error) {
	return Status{
		UserID:                s.UserID,
		PlanCode:              s.PlanCode,
		Phase:                 string(s.Phase),
		PeriodStart:           s.Period.Start,
		PeriodEnd:             s.Period.End,
		RenewalCount:          s.RenewalCount,
		Context:               s.Context.Clone(),
		CancelRequested:       s.CancelRequested,
		DunningAttempt:        s.DunningAttempt,
		LastChargedAt:         s.LastChargedAt,
		LastChargeAmountCents: s.LastChargeAmountCents,
		LastFailureAt:         s.LastFailureAt,
		LastFailureReason:     s.LastFailureReason,
		TotalChargedCents:     s.TotalChargedCents,
		SuccessfulChargeCount: s.SuccessfulChargeCount,
		FailedChargeCount:     s.FailedChargeCount,
	}, nil
}

// HandleCancelSignal flips the cancel flag. The main loop reads it on the
// next selector wakeup. Phase doesn't transition here — that happens in
// AwaitPeriodEndOrCancellation (or Trial).
func (s *Subscription) HandleCancelSignal(_ workflow.Context, _ struct{}) {
	s.CancelRequested = true
}

// HandleContextUpdateSignal merges integrator-supplied context updates into
// the subscription's mutable bag.
func (s *Subscription) HandleContextUpdateSignal(_ workflow.Context, updates map[string]string) {
	if s.Context == nil {
		s.Context = subscription.Context{}
	}
	for k, v := range updates {
		s.Context[k] = v
	}
}

// registerHandlers wires up query + signal handlers. The Activate UPDATE
// handler is registered separately inside AwaitActivation (T17) because it's
// only valid on the first paid period.
func (s *Subscription) registerHandlers(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, QuerySubscriptionStatus, s.AsStatus); err != nil {
		return err
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, SignalCancelSubscription)
		for {
			var v struct{}
			ch.Receive(ctx, &v)
			s.HandleCancelSignal(ctx, v)
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, SignalUpdateContext)
		for {
			var updates map[string]string
			ch.Receive(ctx, &updates)
			s.HandleContextUpdateSignal(ctx, updates)
		}
	})
	return nil
}
