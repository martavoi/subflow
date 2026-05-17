package workflow

import (
	"testing"
	"time"

	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/hook"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// sampleInput builds a SubscriptionInput with all 11 hooks enabled so test
// observers can count fire counts. Period anchored at time.Now to play nicely
// with the test harness mock clock.
func sampleInput(mods ...func(*plan.Plan)) SubscriptionInput {
	start := time.Now().UTC().Truncate(time.Second)
	p := plan.Plan{
		Code:                "monthly-basic",
		Cadence:             30 * 24 * time.Hour,
		PriceCents:          999,
		Currency:            "USD",
		IntegrationEndpoint: "mock:50052",
		EnabledHooks: []hook.Type{
			hook.TrialStarted, hook.TrialWillEnd,
			hook.RenewalUpcoming,
			hook.Activated, hook.Renewed,
			hook.PastDue, hook.Recovered,
			hook.Canceled, hook.Deactivated,
			hook.PaymentOK, hook.PaymentFailed,
		},
	}
	for _, m := range mods {
		m(&p)
	}
	return SubscriptionInput{
		SubscriptionID: "sub-1",
		IntervalID:     "interval-1",
		UserID:         "user-1",
		PlanID:         "plan-1",
		Plan:           p,
		PeriodStart:    start,
		PeriodEnd:      start.Add(p.Cadence),
		Context:        Context{"card_id": "card_001"},
	}
}

type hookRecord struct {
	lifecycle map[string]int
	payment   map[string]int
}

func newHookRecord() *hookRecord {
	return &hookRecord{lifecycle: map[string]int{}, payment: map[string]int{}}
}

// registerMocks registers activity mocks for all 3 registered activity names.
// chargeBehavior, if provided, is called for each ChargePayment attempt with
// the 1-based attempt index; return nil for success or an error to fail.
func registerMocks(env *testsuite.TestWorkflowEnvironment, rec *hookRecord, chargeBehavior func(attempt int) error) {
	chargeAttempts := 0
	env.RegisterActivityWithOptions(
		func(in ChargePayment) (ChargeResult, error) {
			chargeAttempts++
			if chargeBehavior != nil {
				if err := chargeBehavior(chargeAttempts); err != nil {
					return ChargeResult{}, err
				}
			}
			return ChargeResult{
				Reference: in.Reference, TransactionID: "txn", AmountCents: in.AmountCents, Currency: in.Currency,
			}, nil
		},
		activity.RegisterOptions{Name: "ChargePayment"},
	)
	env.RegisterActivityWithOptions(
		func(_ billing.Event) error { return nil },
		activity.RegisterOptions{Name: "RecordBillingEvent"},
	)

	mockDispatch := func(in DispatchHook) error {
		if in.Lifecycle != nil {
			rec.lifecycle[string(in.Type)]++
		} else if in.Payment != nil {
			rec.payment[string(in.Type)]++
		}
		return nil
	}
	env.RegisterActivityWithOptions(mockDispatch, activity.RegisterOptions{Name: "DispatchHook"})
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

	input := sampleInput(func(p *plan.Plan) {
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

	input := sampleInput(func(p *plan.Plan) {
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

	input := sampleInput(func(p *plan.Plan) {
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

	input := sampleInput(func(p *plan.Plan) {
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

// TestRenewalUpcoming_FiresOnRenewalPeriod verifies that the renewal_upcoming
// hook fires exactly once per renewal period when RenewalUpcomingBefore > 0.
func TestRenewalUpcoming_FiresOnRenewalPeriod(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	// Renewal period: RenewalCount=1 so Run() goes directly to Renew → AwaitEnd.
	// Cadence=30s, RenewalUpcomingBefore=5s → notice fires at PeriodEnd-5s.
	input := sampleInput(func(p *plan.Plan) {
		p.Cadence = 30 * time.Second
		p.RenewalUpcomingBefore = 5 * time.Second
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.renewal_upcoming"] != 1 {
		t.Fatalf("expected 1 renewal_upcoming hook, got %d", rec.lifecycle["subscription.renewal_upcoming"])
	}
}

// TestRenewalUpcoming_FiresOnFirstPaidPeriod verifies that the renewal_upcoming
// hook fires during the first paid period (no-trial plan, RenewalCount=0 after
// activation charges the first period).
func TestRenewalUpcoming_FiresOnFirstPaidPeriod(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	// No-trial plan: RenewalCount=0, Activate update triggers first charge,
	// then AwaitEnd runs with RenewalUpcomingBefore set.
	input := sampleInput(func(p *plan.Plan) {
		p.Cadence = 30 * time.Second
		p.RenewalUpcomingBefore = 5 * time.Second
	})
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	// Send Activate update at t=0 so activation proceeds immediately.
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "act-1", t)
	}, 0)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.renewal_upcoming"] != 1 {
		t.Fatalf("expected 1 renewal_upcoming hook on first paid period, got %d", rec.lifecycle["subscription.renewal_upcoming"])
	}
}

// TestRenewalUpcoming_DoesNotFireWhenZero verifies that the hook does NOT fire
// when RenewalUpcomingBefore == 0.
func TestRenewalUpcoming_DoesNotFireWhenZero(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	input := sampleInput(func(p *plan.Plan) {
		p.Cadence = 30 * time.Second
		// RenewalUpcomingBefore is zero (default) — no hook should fire.
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.renewal_upcoming"] != 0 {
		t.Fatalf("expected 0 renewal_upcoming hooks when RenewalUpcomingBefore==0, got %d", rec.lifecycle["subscription.renewal_upcoming"])
	}
}

// TestRenewalUpcoming_DoesNotFireWhenCanceledBeforeNotice verifies that the
// hook does NOT fire if cancel arrives before the notice timestamp.
func TestRenewalUpcoming_DoesNotFireWhenCanceledBeforeNotice(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	rec := newHookRecord()
	registerMocks(env, rec, nil)

	// Cadence=30s, notice at PeriodEnd-5s = t+25s.
	// Cancel arrives at t+10s — before the notice at t+25s.
	input := sampleInput(func(p *plan.Plan) {
		p.Cadence = 30 * time.Second
		p.RenewalUpcomingBefore = 5 * time.Second
	})
	input.RenewalCount = 1
	input.PeriodEnd = input.PeriodStart.Add(input.Plan.Cadence)

	// Signal cancel at 10s into the period — well before the 25s notice.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancelSubscription, struct{}{})
	}, 10*time.Second)

	env.ExecuteWorkflow(SubscriptionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if rec.lifecycle["subscription.renewal_upcoming"] != 0 {
		t.Fatalf("renewal_upcoming should NOT fire when cancel precedes notice timestamp, got %d", rec.lifecycle["subscription.renewal_upcoming"])
	}
	if rec.lifecycle["subscription.canceled"] != 1 {
		t.Fatalf("expected canceled hook, got %d", rec.lifecycle["subscription.canceled"])
	}
	if rec.lifecycle["subscription.deactivated"] != 1 {
		t.Fatalf("expected deactivated hook, got %d", rec.lifecycle["subscription.deactivated"])
	}
}
