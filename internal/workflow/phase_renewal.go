package workflow

import (
	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// Renew runs the renewal-period activities: charge → record event → fire
// payment + renewed hooks. Returns the original charge error so the caller
// (Run) can route to Dun on failure.
func (s *Subscription) Renew(ctx workflow.Context) error {
	if err := s.Charge(ctx, chargeRenewal, 0); err != nil {
		return err
	}
	s.transitionTo(ctx, PhaseActive)
	_ = s.emitLifecycle(ctx, hook.Renewed)
	return nil
}
