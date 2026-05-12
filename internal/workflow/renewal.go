package workflow

import "go.temporal.io/sdk/workflow"

// Renew runs the renewal-period activities: charge → record event → fire
// payment + renewed hooks. Returns the original charge error so the caller
// (Run) can route to HandleDunning on failure.
func (s *Subscription) Renew(ctx workflow.Context) error {
	if err := s.Charge(ctx, chargeRenewal, 0); err != nil {
		return err
	}
	s.transitionTo(ctx, PhaseActive)
	_ = s.FireLifecycleHook(ctx, HookRenewed)
	return nil
}
