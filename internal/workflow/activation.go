package workflow

import "go.temporal.io/sdk/workflow"

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
	_ = s.FireLifecycleHook(ctx, HookActivated)
	return nil
}
