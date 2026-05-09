package workflow

import (
	"testing"
	"time"

	activityPkg "github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func sampleInput() subscription.SubscriptionInput {
	// Use time.Now() so the period is always in the future relative to the
	// test harness mock clock (which starts at wall-clock time).
	start := time.Now().UTC().Truncate(time.Second)
	return subscription.SubscriptionInput{
		SubscriptionID:  "sub-1",
		UserID:          "user-1",
		PlanID:          "plan-1",
		PlanCode:        "monthly-basic",
		BillingInterval: 30 * 24 * time.Hour,
		IntegrationHost: "mock:50052",
		PriceCents:      999,
		PeriodStart:     start,
		PeriodEnd:       start.Add(30 * 24 * time.Hour),
		Context:         subscription.Context{"card_id": "card_001"},
	}
}

func TestSubscriptionWorkflow_HappyActivation_ContinuesAsNew(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	registerActivityMocks(env, nil)

	// Activation runs via the Activate update; send it as soon as the workflow
	// starts so it can register its update handler and process the request.
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "activation-1", t)
	}, 0)

	env.ExecuteWorkflow(SubscriptionWorkflow, sampleInput())

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatalf("expected ContinueAsNew error, got nil")
	}
}

func TestSubscriptionWorkflow_CancelMidPeriod_RunsDeactivation(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	deactivateCalled := false
	registerActivityMocks(env, func(eventType string) {
		if eventType == activityPkg.EventTypeDeactivate {
			deactivateCalled = true
		}
	})

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(UpdateActivate, "activation-1", t)
	}, 0)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancelSubscription, nil)
	}, 5*24*time.Hour)

	env.ExecuteWorkflow(SubscriptionWorkflow, sampleInput())

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if !deactivateCalled {
		t.Fatalf("expected deactivation event to be published, none observed")
	}
}

func registerActivityMocks(env *testsuite.TestWorkflowEnvironment, onPublish func(eventType string)) {
	env.RegisterActivityWithOptions(
		func(in activityPkg.ChargePaymentInput) (activityPkg.ChargePaymentResult, error) {
			return activityPkg.ChargePaymentResult{Reference: in.Reference, TransactionID: "txn", AmountCents: in.PriceCents}, nil
		},
		activity.RegisterOptions{Name: "ChargePayment"},
	)
	env.RegisterActivityWithOptions(
		func(in activityPkg.PublishEventInput) error {
			if onPublish != nil {
				onPublish(in.EventType)
			}
			return nil
		},
		activity.RegisterOptions{Name: "PublishSubscriptionEvent"},
	)
	env.RegisterActivityWithOptions(
		func(in activityPkg.IntegrationCallInput) (activityPkg.IntegrationCallResult, error) {
			out := subscription.Context{}
			for k, v := range in.Context {
				out[k] = v
			}
			out["last_event"] = in.EventType
			return activityPkg.IntegrationCallResult{UpdatedContext: out}, nil
		},
		activity.RegisterOptions{Name: "NotifyIntegrationService"},
	)
	env.RegisterActivityWithOptions(
		func(_ activityPkg.ProjectionUpdate) error { return nil },
		activity.RegisterOptions{Name: "UpdateSubscriptionProjection"},
	)
}
