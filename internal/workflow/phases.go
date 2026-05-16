package workflow

import (
	"errors"
	"time"

	"github.com/google/uuid"
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

// ErrDunningExhausted indicates all dunning retries failed. The caller (Run)
// transitions the subscription to deactivated.
var ErrDunningExhausted = errors.New("dunning exhausted")

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
	_ = s.FireLifecycleHook(ctx, hook.TrialStarted)

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
			_ = s.FireLifecycleHook(ctx, hook.TrialWillEnd)
		}
	}

	if s.CancelRequested {
		return trialOutcomeCanceled, nil
	}
	return trialOutcomeEnded, nil
}

// AwaitActivation registers the Activate update handler and blocks on
// workflow.Await until activation completes. The handler runs Activate(),
// which performs the first-period charge + hooks. Returns nil on success,
// or the activation error if the charge failed terminally.
//
// The Activate update is sent by the API immediately after starting the
// workflow via client.UpdateWithStartWorkflow.
func (s *Subscription) AwaitActivation(ctx workflow.Context) error {
	activated := false
	var activationErr error

	if err := workflow.SetUpdateHandler(ctx, UpdateActivate,
		func(ctx workflow.Context) (ActivationResult, error) {
			if err := s.Activate(ctx); err != nil {
				activationErr = err
				activated = true // unblock the Await; Run will deactivate
				return ActivationResult{}, err
			}
			activated = true
			return ActivationResult{
				Phase:   string(s.Phase),
				Context: s.Context.Clone(),
			}, nil
		},
	); err != nil {
		return err
	}

	if err := workflow.Await(ctx, func() bool { return activated }); err != nil {
		return err
	}
	return activationErr
}

// Activate runs the first-period activation activities: charge → record event →
// fire payment + lifecycle hooks. Internal — invoked from AwaitActivation's
// update handler (no-trial case) and from the trial-to-paid transition.
func (s *Subscription) Activate(ctx workflow.Context) error {
	if err := s.Charge(ctx, chargeActivation, 0); err != nil {
		return err
	}
	s.transitionTo(ctx, PhaseActive)
	_ = s.FireLifecycleHook(ctx, hook.Activated)
	return nil
}

// Renew runs the renewal-period activities: charge → record event → fire
// payment + renewed hooks. Returns the original charge error so the caller
// (Run) can route to Dun on failure.
func (s *Subscription) Renew(ctx workflow.Context) error {
	if err := s.Charge(ctx, chargeRenewal, 0); err != nil {
		return err
	}
	s.transitionTo(ctx, PhaseActive)
	_ = s.FireLifecycleHook(ctx, hook.Renewed)
	return nil
}

// Dun runs the retry loop after a failed renewal charge. Transitions
// to past_due on entry; on each retry calls Charge() with the current
// DunningAttempt; recovers to active on success; returns ErrDunningExhausted
// if all attempts fail.
func (s *Subscription) Dun(ctx workflow.Context) error {
	s.transitionTo(ctx, PhasePastDue)
	_ = s.FireLifecycleHook(ctx, hook.PastDue)

	for s.DunningAttempt < s.Plan.DunningMaxAttempts {
		s.DunningAttempt++

		// Exponential backoff: initial * 2^(attempt-1).
		backoff := s.Plan.DunningRetryBackoff * time.Duration(1<<(s.DunningAttempt-1))
		_ = workflow.Sleep(ctx, backoff)

		if err := s.Charge(ctx, chargeDunning, s.DunningAttempt); err == nil {
			// Recovered.
			s.DunningAttempt = 0
			s.transitionTo(ctx, PhaseActive)
			_ = s.FireLifecycleHook(ctx, hook.Recovered)
			return nil
		}
		// Charge failed again — loop.
	}

	return ErrDunningExhausted
}

// AwaitEnd blocks until either the period timer fires or a cancel signal
// arrives. End-of-period semantics: if cancel arrives early, the workflow
// transitions to canceled, fires the canceled hook, and sleeps the remainder
// of the period before returning true.
//
// Returns true if canceled, false on natural period end.
func (s *Subscription) AwaitEnd(ctx workflow.Context) bool {
	// If cancel was already requested (e.g., signal during dunning), transition
	// immediately and sleep out the remainder.
	if s.CancelRequested {
		s.transitionTo(ctx, PhaseCanceled)
		_ = s.FireLifecycleHook(ctx, hook.Canceled)
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
	_ = s.FireLifecycleHook(ctx, hook.Canceled)
	if remaining := s.Period.End.Sub(workflow.Now(ctx)); remaining > 0 {
		_ = workflow.Sleep(ctx, remaining)
	}
	return true
}

// Deactivate runs the terminal deactivation hook and transitions to deactivated.
// After this returns, the workflow run completes (no CAN).
func (s *Subscription) Deactivate(ctx workflow.Context) error {
	s.transitionTo(ctx, PhaseDeactivated)
	_ = s.FireLifecycleHook(ctx, hook.Deactivated)
	return nil
}

// NextPeriod CANs the workflow with the next period's input.
// Generates a fresh IntervalID via SideEffect (UUID is non-deterministic, so
// it must be recorded in history) and upserts the SubflowPeriodEnd search
// attribute so the new period boundary is visible immediately for
// "expiring soon" queries.
func (s *Subscription) NextPeriod(ctx workflow.Context) error {
	var nextIntervalID string
	_ = workflow.SideEffect(ctx, func(workflow.Context) any {
		return uuid.NewString()
	}).Get(&nextIntervalID)

	next := NextBillingPeriod(s.toInput(), nextIntervalID)
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		subflowtemporal.KeyPeriodEnd.ValueSet(next.PeriodEnd),
	)
	return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, next)
}

// toInput serializes the entity's relevant state back into a
// SubscriptionInput for CAN. The next run reconstructs from this.
// IntervalID is left zero — the caller supplies a fresh one.
func (s *Subscription) toInput() SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  s.SubscriptionID,
		UserID:          s.UserID,
		PlanID:          s.PlanID,
		Plan:            s.Plan,
		PeriodStart:     s.Period.Start,
		PeriodEnd:       s.Period.End,
		Context:         s.Context.Clone(),
		RenewalCount:    s.RenewalCount,
		CancelRequested: s.CancelRequested,
	}
}
