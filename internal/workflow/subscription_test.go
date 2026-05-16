package workflow

import (
	"testing"
	"time"

	activityPkg "github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// sampleInput builds a SubscriptionInput with all 10 hooks enabled so test
// observers can count fire counts. Period anchored at time.Now to play nicely
// with the test harness mock clock.
func sampleInput(mods ...func(*plan.Snapshot)) subscription.SubscriptionInput {
	start := time.Now().UTC().Truncate(time.Second)
	p := plan.Snapshot{
		Code:                "monthly-basic",
		Cadence:             30 * 24 * time.Hour,
		PriceCents:          999,
		Currency:            "USD",
		IntegrationEndpoint: "mock:50052",
		EnabledHooks: []hook.Type{
			hook.TrialStarted, hook.TrialWillEnd,
			hook.Activated, hook.Renewed,
			hook.PastDue, hook.Recovered,
			hook.Canceled, hook.Deactivated,
			hook.PaymentOK, hook.PaymentFailed,
		},
	}
	for _, m := range mods {
		m(&p)
	}
	return subscription.SubscriptionInput{
		SubscriptionID: "sub-1",
		IntervalID:     "interval-1",
		UserID:         "user-1",
		PlanID:         "plan-1",
		Plan:           p,
		PeriodStart:    start,
		PeriodEnd:      start.Add(p.Cadence),
		Context:        subscription.Context{"card_id": "card_001"},
	}
}

type hookRecord struct {
	lifecycle map[string]int
	payment   map[string]int
}

func newHookRecord() *hookRecord {
	return &hookRecord{lifecycle: map[string]int{}, payment: map[string]int{}}
}

// registerMocks registers activity mocks for all 12 registered activity names.
// chargeBehavior, if provided, is called for each ChargePayment attempt with
// the 1-based attempt index; return nil for success or an error to fail.
func registerMocks(env *testsuite.TestWorkflowEnvironment, rec *hookRecord, chargeBehavior func(attempt int) error) {
	chargeAttempts := 0
	env.RegisterActivityWithOptions(
		func(in activityPkg.ChargePaymentInput) (activityPkg.ChargePaymentResult, error) {
			chargeAttempts++
			if chargeBehavior != nil {
				if err := chargeBehavior(chargeAttempts); err != nil {
					return activityPkg.ChargePaymentResult{}, err
				}
			}
			return activityPkg.ChargePaymentResult{
				Reference: in.Reference, TransactionID: "txn", AmountCents: in.AmountCents, Currency: in.Currency,
			}, nil
		},
		activity.RegisterOptions{Name: "ChargePayment"},
	)
	env.RegisterActivityWithOptions(
		func(_ billing.Event) error { return nil },
		activity.RegisterOptions{Name: "RecordBillingEvent"},
	)

	mkLifecycle := func(name string) func(activityPkg.LifecycleHookInput) error {
		return func(_ activityPkg.LifecycleHookInput) error {
			rec.lifecycle[name]++
			return nil
		}
	}
	mkPayment := func(name string) func(activityPkg.PaymentHookInput) error {
		return func(_ activityPkg.PaymentHookInput) error {
			rec.payment[name]++
			return nil
		}
	}
	env.RegisterActivityWithOptions(mkLifecycle("subscription.trial_started"), activity.RegisterOptions{Name: "OnTrialStarted"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.trial_will_end"), activity.RegisterOptions{Name: "OnTrialWillEnd"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.activated"), activity.RegisterOptions{Name: "OnActivated"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.renewed"), activity.RegisterOptions{Name: "OnRenewed"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.past_due"), activity.RegisterOptions{Name: "OnPastDue"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.recovered"), activity.RegisterOptions{Name: "OnRecovered"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.canceled"), activity.RegisterOptions{Name: "OnCanceled"})
	env.RegisterActivityWithOptions(mkLifecycle("subscription.deactivated"), activity.RegisterOptions{Name: "OnDeactivated"})
	env.RegisterActivityWithOptions(mkPayment("payment.succeeded"), activity.RegisterOptions{Name: "OnPaymentSucceeded"})
	env.RegisterActivityWithOptions(mkPayment("payment.failed"), activity.RegisterOptions{Name: "OnPaymentFailed"})
}

func TestSubscription_HappyActivation_ContinuesAsNew(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	// Send the Activate update at t=0 so activation can complete.
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "act-1", t)
	}, 0)

	env.ExecuteWorkflow(SubscriptionWorkflow, sampleInput())

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	// ContinueAsNew is surfaced as a non-nil error to the test harness.
	if err := env.GetWorkflowError(); err == nil {
		t.Fatalf("expected ContinueAsNew error, got nil")
	}
	if rec.lifecycle["subscription.activated"] != 1 {
		t.Fatalf("expected 1 activated hook, got %d", rec.lifecycle["subscription.activated"])
	}
	if rec.payment["payment.succeeded"] != 1 {
		t.Fatalf("expected 1 payment.succeeded hook, got %d", rec.payment["payment.succeeded"])
	}
}

func TestSubscription_TrialThenActivate(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	input := sampleInput(func(p *plan.Snapshot) {
		p.TrialDuration = 24 * time.Hour
		p.TrialEndNoticeBefore = 2 * time.Hour
	})
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.TrialDuration)

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "act-1", t)
	}, 24*time.Hour+10*time.Minute)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.trial_started"] != 1 {
		t.Fatalf("expected trial_started, got %d", rec.lifecycle["subscription.trial_started"])
	}
	if rec.lifecycle["subscription.trial_will_end"] != 1 {
		t.Fatalf("expected trial_will_end, got %d", rec.lifecycle["subscription.trial_will_end"])
	}
	if rec.lifecycle["subscription.activated"] != 1 {
		t.Fatalf("expected activated, got %d", rec.lifecycle["subscription.activated"])
	}
}

func TestSubscription_TrialCanceled(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	input := sampleInput(func(p *plan.Snapshot) {
		p.TrialDuration = 24 * time.Hour
	})
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.TrialDuration)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancelSubscription, struct{}{})
	}, 6*time.Hour)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if rec.payment["payment.succeeded"] != 0 {
		t.Fatalf("trial-canceled should not have charged")
	}
	if rec.lifecycle["subscription.deactivated"] != 1 {
		t.Fatalf("expected deactivated, got %d", rec.lifecycle["subscription.deactivated"])
	}
}

func TestSubscription_RenewalDunningRecovery(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()

	// Renewal first charge fails (non-retryable so Temporal doesn't retry at
	// SDK level), then dunning attempt 1 succeeds.
	registerMocks(env, rec, func(attempt int) error {
		if attempt == 1 {
			return temporal.NewNonRetryableApplicationError("transient first try", "TestTransientNonRetryable", nil)
		}
		return nil
	})

	input := sampleInput(func(p *plan.Snapshot) {
		p.DunningMaxAttempts = 3
		p.DunningRetryBackoff = 1 * time.Hour
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.past_due"] != 1 {
		t.Fatalf("expected past_due, got %d", rec.lifecycle["subscription.past_due"])
	}
	if rec.lifecycle["subscription.recovered"] != 1 {
		t.Fatalf("expected recovered, got %d", rec.lifecycle["subscription.recovered"])
	}
}

func TestSubscription_RenewalDunningExhausted(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()

	// Every charge fails non-retryably — dunning exhausts.
	registerMocks(env, rec, func(_ int) error {
		return temporal.NewNonRetryableApplicationError("transient first try", "TestTransientNonRetryable", nil)
	})

	input := sampleInput(func(p *plan.Snapshot) {
		p.DunningMaxAttempts = 2
		p.DunningRetryBackoff = 1 * time.Hour
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.deactivated"] != 1 {
		t.Fatalf("expected deactivated, got %d", rec.lifecycle["subscription.deactivated"])
	}
}
