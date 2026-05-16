package workflow

import "go.temporal.io/sdk/workflow"

// AwaitPeriodEndOrCancellation blocks until either the period timer fires or
// a cancel signal arrives. End-of-period semantics: if cancel arrives early,
// the workflow transitions to canceled, fires the canceled hook, and sleeps
// the remainder of the period before returning true.
//
// Returns true if canceled, false on natural period end.
func (s *Subscription) AwaitPeriodEndOrCancellation(ctx workflow.Context) bool {
	// If cancel was already requested (e.g., signal during dunning), transition
	// immediately and sleep out the remainder.
	if s.CancelRequested {
		s.transitionTo(ctx, PhaseCanceled)
		_ = s.FireLifecycleHook(ctx, HookCanceled)
		if remaining := s.Period.End.Sub(workflow.Now(ctx)); remaining > 0 {
			_ = workflow.Sleep(ctx, remaining)
		}
		return true
	}

	now := workflow.Now(ctx)
	if !s.Period.End.After(now) {
		return s.CancelRequested
	}

	// AwaitWithTimeout wakes when condition() becomes true (cancel signal set
	// CancelRequested) or the timeout elapses. ok=false means timed out
	// (period ended naturally); ok=true means cancel arrived first.
	ok, _ := workflow.AwaitWithTimeout(ctx, s.Period.End.Sub(now), func() bool {
		return s.CancelRequested
	})

	// ok=false: timer fired before cancel — period ended naturally.
	if !ok {
		return false
	}

	// Cancel arrived during the wait. Mark canceled, fire hook, sleep the
	// remainder, then return true.
	s.transitionTo(ctx, PhaseCanceled)
	_ = s.FireLifecycleHook(ctx, HookCanceled)
	if remaining := s.Period.End.Sub(workflow.Now(ctx)); remaining > 0 {
		_ = workflow.Sleep(ctx, remaining)
	}
	return true
}

// Deactivate runs the terminal deactivation hook and transitions to deactivated.
// After this returns, the workflow run completes (no CAN).
func (s *Subscription) Deactivate(ctx workflow.Context) error {
	s.transitionTo(ctx, PhaseDeactivated)
	_ = s.FireLifecycleHook(ctx, HookDeactivated)
	return nil
}
