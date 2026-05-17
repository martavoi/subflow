package workflow

import "go.temporal.io/sdk/workflow"

// View is the query handler — returns the current entity snapshot for API
// rendering. Pure read; no mutation. Bound to s for use as a method-value
// handler.
func (s *Subscription) View() (View, error) {
	return View{
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

// OnCancel flips the cancel flag. The main loop reads it on the next selector
// wakeup. Phase doesn't transition here — that happens in AwaitEnd (or Trial).
func (s *Subscription) OnCancel(_ workflow.Context, _ struct{}) {
	s.CancelRequested = true
}

// OnContextUpdate merges integrator-supplied context updates into the
// subscription's mutable bag.
func (s *Subscription) OnContextUpdate(_ workflow.Context, updates map[string]string) {
	if s.Context == nil {
		s.Context = Context{}
	}
	for k, v := range updates {
		s.Context[k] = v
	}
}

// registerMessageHandlers wires up the query + signal handlers that live for
// the workflow's whole lifetime. Called once from SubscriptionWorkflow before
// Run takes over — matches the canonical Temporal entity-workflow pattern of
// registering lifetime-scoped handlers at construction time. The Activate
// UPDATE handler is registered separately inside AwaitActivation because
// it's only valid on the first paid period.
func (s *Subscription) registerMessageHandlers(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, QuerySubscriptionView, s.View); err != nil {
		return err
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, SignalCancelSubscription)
		for {
			var v struct{}
			ch.Receive(ctx, &v)
			s.OnCancel(ctx, v)
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, SignalUpdateContext)
		for {
			var updates map[string]string
			ch.Receive(ctx, &updates)
			s.OnContextUpdate(ctx, updates)
		}
	})
	return nil
}
