package workflow

import (
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
//     even if some time remains on the trial (trial cancels are immediate;
//     end-of-period semantics apply only to paid periods)
func (s *Subscription) Trial(ctx workflow.Context) (trialOutcome, error) {
	s.transitionTo(ctx, PhaseTrialing)
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		subflowtemporal.KeyTrialEnd.ValueSet(s.Period.End),
	)
	_ = s.FireLifecycleHook(ctx, HookTrialStarted)

	now := workflow.Now(ctx)
	if !s.Period.End.After(now) {
		// Trial already over (shouldn't happen on a fresh subscription, but
		// be defensive).
		return trialOutcomeEnded, nil
	}

	// Optional advance-notice timer. If TrialEndNoticeBefore > 0 and the
	// notice time is in the future, schedule a notice timer alongside the
	// end timer; fire the notice hook when it elapses.
	var noticeFut workflow.Future
	if s.Plan.TrialEndNoticeBefore > 0 {
		noticeAt := s.Period.End.Add(-s.Plan.TrialEndNoticeBefore)
		if noticeAt.After(now) {
			noticeFut = workflow.NewTimer(ctx, noticeAt.Sub(now))
		}
	}
	endFut := workflow.NewTimer(ctx, s.Period.End.Sub(now))

	noticeFired := noticeFut == nil // skip if not configured
	ended := false

	for !ended && !s.CancelRequested {
		sel := workflow.NewSelector(ctx)
		if !noticeFired && noticeFut != nil {
			sel.AddFuture(noticeFut, func(workflow.Future) {
				noticeFired = true
			})
		}
		sel.AddFuture(endFut, func(workflow.Future) {
			ended = true
		})
		sel.Select(ctx)

		// If the notice just fired, dispatch the hook then loop again to wait
		// for the end timer (or cancel).
		if noticeFired && !ended && !s.CancelRequested {
			// dispatch only once; the predicate above prevents re-add to selector
			_ = s.FireLifecycleHook(ctx, HookTrialWillEnd)
		}
	}

	if s.CancelRequested {
		return trialOutcomeCanceled, nil
	}
	return trialOutcomeEnded, nil
}
