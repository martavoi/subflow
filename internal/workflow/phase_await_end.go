package workflow

import (
	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// AwaitEnd blocks until either the period timer fires or a cancel signal
// arrives. End-of-period semantics: if cancel arrives early, the workflow
// transitions to canceled, fires the canceled hook, and sleeps the remainder
// of the period before returning true.
//
// If Plan.RenewalUpcomingBefore > 0 and the notice timestamp is in the future,
// a two-phase wait is used: first wait until the notice timestamp (firing
// hook.RenewalUpcoming on arrival), then wait until the period end. Cancel
// at any point during either phase takes the cancel path.
//
// Returns true if canceled, false on natural period end.
func (s *Subscription) AwaitEnd(ctx workflow.Context) bool {
	// If cancel was already requested (e.g., signal during dunning), transition
	// immediately and sleep out the remainder.
	if s.CancelRequested {
		return s.cancelAndSleep(ctx)
	}

	now := workflow.Now(ctx)
	if !s.Period.End.After(now) {
		return s.CancelRequested
	}

	// Phase 1: optional renewal-upcoming notice.
	if s.Plan.RenewalUpcomingBefore > 0 {
		noticeAt := s.Period.End.Add(-s.Plan.RenewalUpcomingBefore)
		if noticeAt.After(now) {
			// Wait until noticeAt or cancel.
			ok, _ := workflow.AwaitWithTimeout(ctx, noticeAt.Sub(now), func() bool {
				return s.CancelRequested
			})
			if ok {
				// Cancel arrived before notice timestamp.
				return s.cancelAndSleep(ctx)
			}
			// Notice timestamp reached — fire the hook and continue to phase 2.
			_ = s.emitLifecycle(ctx, hook.RenewalUpcoming)
			now = workflow.Now(ctx)
		}
	}

	// Phase 2: wait until period end or cancel.
	ok, _ := workflow.AwaitWithTimeout(ctx, s.Period.End.Sub(now), func() bool {
		return s.CancelRequested
	})
	if !ok {
		// Timer fired before cancel — period ended naturally.
		return false
	}
	return s.cancelAndSleep(ctx)
}

// cancelAndSleep handles the cancel-during-period path: transitions to
// canceled, fires the canceled hook, sleeps the remaining period, returns true.
func (s *Subscription) cancelAndSleep(ctx workflow.Context) bool {
	s.transitionTo(ctx, PhaseCanceled)
	_ = s.emitLifecycle(ctx, hook.Canceled)
	if remaining := s.Period.End.Sub(workflow.Now(ctx)); remaining > 0 {
		_ = workflow.Sleep(ctx, remaining)
	}
	return true
}
