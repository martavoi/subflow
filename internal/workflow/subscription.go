package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/workflow"
)

// Phase enumerates the lifecycle states of a Subscription.
type Phase string

const (
	PhaseStarting    Phase = "starting"
	PhaseTrialing    Phase = "trialing"
	PhaseActive      Phase = "active"
	PhasePastDue     Phase = "past_due"
	PhaseCanceled    Phase = "canceled"
	PhaseDeactivated Phase = "deactivated"
)

// BillingPeriod is the half-open interval [Start, End) the subscription is
// currently billing for.
type BillingPeriod struct {
	Start time.Time
	End   time.Time
}

// Subscription is the entity-workflow aggregate root. Every domain verb is a
// method. All state lives in fields. No I/O — activities do that.
type Subscription struct {
	// Identity (immutable)
	ID       string
	UserID   string
	PlanID   string
	PlanCode string

	// Plan snapshot — immutable for this run
	Plan plan.Snapshot

	// Lifecycle state
	Phase           Phase
	Period          BillingPeriod
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool

	// Dunning state
	DunningAttempt int

	// Billing summary (O(1), updated on each charge attempt)
	LastChargedAt         time.Time
	LastChargeAmountCents int64
	LastFailureAt         time.Time
	LastFailureReason     string
	TotalChargedCents     int64
	SuccessfulChargeCount int
	FailedChargeCount     int
}

// NewSubscription builds a fresh Subscription entity from workflow input.
// Pure; safe to call during workflow replay.
func NewSubscription(in subscription.SubscriptionInput) *Subscription {
	return &Subscription{
		ID:              in.SubscriptionID,
		UserID:          in.UserID,
		PlanID:          in.PlanID,
		PlanCode:        in.Plan.Code,
		Plan:            in.Plan,
		Phase:           PhaseStarting,
		Period:          BillingPeriod{Start: in.PeriodStart, End: in.PeriodEnd},
		RenewalCount:    in.RenewalCount,
		Context:         in.Context.Clone(),
		CancelRequested: in.CancelRequested,
	}
}

// SubscriptionWorkflow is the top-level workflow function registered with
// the worker. Real lifecycle logic in (*Subscription).Run (filled by T21).
func SubscriptionWorkflow(ctx workflow.Context, in subscription.SubscriptionInput) error {
	return NewSubscription(in).Run(ctx)
}

// Run is the workflow body. Stub for now — populated by T21 once all
// lifecycle methods exist.
func (s *Subscription) Run(ctx workflow.Context) error {
	_ = ctx
	return nil
}

// ActivationResult is the response to the UpdateActivate update — returned
// synchronously to the API caller via UpdateWithStartWorkflow.
type ActivationResult struct {
	Phase   string
	Context subscription.Context
}

// transitionTo moves the entity into a new lifecycle phase AND publishes the
// change via the SubflowPhase search attribute. Co-locating these two enforces
// the invariant: every phase change is observable from outside the workflow.
func (s *Subscription) transitionTo(ctx workflow.Context, p Phase) {
	s.Phase = p
	_ = workflow.UpsertSearchAttributes(ctx, map[string]any{
		subflowtemporal.AttrPhase: string(p),
	})
}

// idempotencyKey builds the activity idempotency token. Stable across retries
// within a run; unique across runs (run ID changes each CAN).
func (s *Subscription) idempotencyKey(ctx workflow.Context, purpose string) string {
	info := workflow.GetInfo(ctx)
	return fmt.Sprintf("%s:%s:%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, purpose)
}
