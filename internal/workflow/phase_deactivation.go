package workflow

import (
	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/workflow"
)

// Deactivate runs the terminal deactivation hook and transitions to deactivated.
// After this returns, the workflow run completes (no CAN).
func (s *Subscription) Deactivate(ctx workflow.Context) error {
	s.transitionTo(ctx, PhaseDeactivated)
	_ = s.emitLifecycle(ctx, hook.Deactivated)
	return nil
}
