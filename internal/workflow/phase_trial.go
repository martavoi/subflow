package workflow

import (
	"github.com/martavoi/subflow/internal/hook"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)

// trialOutcome reports how the trial phase ended.
type trialOutcome int

const (
	trialOutcomeEnded    trialOutcome = iota // trial period elapsed naturally
	trialOutcomeCanceled                     // cancel signal received during trial
)

// Trial parks the workflow in trialing phase until either the trial period
// elapses or a cancel signal arrives. Fires subscription.trial_started on
// entry; fires subscription.trial_will_end at TrialEndNoticeBefore (if set).
//
// Semantics:
//   - trial-end timer wins → returns trialOutcomeEnded
//   - cancel signal wins (CancelRequested flag set) → returns trialOutcomeCanceled
//     immediately, even if time remains on the trial (trial cancels are
//     immediate; end-of-period semantics apply only to paid periods)
//
// Implementation: sequential AwaitWithTimeout waits, each racing the period
// timer against the cancel flag. The notice and end events are strictly
// ordered (notice precedes end), so no selector is needed.
func (s *Subscription) Trial(ctx workflow.Context) (trialOutcome, error) {
	s.transitionTo(ctx, PhaseTrialing)
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		subflowtemporal.KeyTrialEnd.ValueSet(s.Period.End),
	)
	_ = s.emitLifecycle(ctx, hook.TrialStarted)

	if s.CancelRequested {
		return trialOutcomeCanceled, nil
	}

	now := workflow.Now(ctx)
	if !s.Period.End.After(now) {
		// Trial already over (shouldn't happen on a fresh subscription, but
		// be defensive).
		return trialOutcomeEnded, nil
	}

	// Phase 1: optional trial-end notice. Cancel during the wait short-circuits.
	if s.Plan.TrialEndNoticeBefore > 0 {
		noticeAt := s.Period.End.Add(-s.Plan.TrialEndNoticeBefore)
		if noticeAt.After(now) {
			cancelled, _ := workflow.AwaitWithTimeout(ctx, noticeAt.Sub(now), func() bool {
				return s.CancelRequested
			})
			if cancelled {
				return trialOutcomeCanceled, nil
			}
			_ = s.emitLifecycle(ctx, hook.TrialWillEnd)
		}
	}

	// Phase 2: wait until trial end or cancel.
	now = workflow.Now(ctx)
	cancelled, _ := workflow.AwaitWithTimeout(ctx, s.Period.End.Sub(now), func() bool {
		return s.CancelRequested
	})
	if cancelled {
		return trialOutcomeCanceled, nil
	}
	return trialOutcomeEnded, nil
}
