package workflow

import (
	"errors"
	"time"

	"go.temporal.io/sdk/workflow"
)

// ErrDunningExhausted indicates all dunning retries failed. The caller (Run)
// transitions the subscription to deactivated.
var ErrDunningExhausted = errors.New("dunning exhausted")

// HandleDunning runs the retry loop after a failed renewal charge. Transitions
// to past_due on entry; on each retry calls Charge() with the current
// DunningAttempt; recovers to active on success; returns ErrDunningExhausted
// if all attempts fail.
func (s *Subscription) HandleDunning(ctx workflow.Context) error {
	s.transitionTo(ctx, PhasePastDue)
	_ = s.FireLifecycleHook(ctx, HookPastDue)

	for s.DunningAttempt < s.Plan.DunningMaxAttempts {
		s.DunningAttempt++

		// Exponential backoff: initial * 2^(attempt-1).
		backoff := s.Plan.DunningRetryBackoff * time.Duration(1<<(s.DunningAttempt-1))
		_ = workflow.Sleep(ctx, backoff)

		if err := s.Charge(ctx, chargeDunning, s.DunningAttempt); err == nil {
			// Recovered.
			s.DunningAttempt = 0
			s.transitionTo(ctx, PhaseActive)
			_ = s.FireLifecycleHook(ctx, HookRecovered)
			return nil
		}
		// Charge failed again — loop.
	}

	return ErrDunningExhausted
}
