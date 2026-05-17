package workflow

import (
	"errors"
	"time"

	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// ErrDunningExhausted indicates all dunning retries failed. The caller (Run)
// transitions the subscription to deactivated.
var ErrDunningExhausted = errors.New("dunning exhausted")

// Dun runs the retry loop after a failed renewal charge. Transitions
// to past_due on entry; on each retry calls Charge() with the current
// DunningAttempt; recovers to active on success; returns ErrDunningExhausted
// if all attempts fail.
func (s *Subscription) Dun(ctx workflow.Context) error {
	s.transitionTo(ctx, PhasePastDue)
	_ = s.emitLifecycle(ctx, hook.PastDue)

	for s.DunningAttempt < s.Plan.DunningMaxAttempts {
		s.DunningAttempt++

		// Exponential backoff: initial * 2^(attempt-1).
		backoff := s.Plan.DunningRetryBackoff * time.Duration(1<<(s.DunningAttempt-1))
		_ = workflow.Sleep(ctx, backoff)

		if err := s.Charge(ctx, chargeDunning, s.DunningAttempt); err == nil {
			// Recovered.
			s.DunningAttempt = 0
			s.transitionTo(ctx, PhaseActive)
			_ = s.emitLifecycle(ctx, hook.Recovered)
			return nil
		}
		// Charge failed again — loop.
	}

	return ErrDunningExhausted
}
