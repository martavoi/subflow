package workflow

import (
	"testing"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
)

func sample() SubscriptionInput {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return SubscriptionInput{
		SubscriptionID: "sub-1",
		UserID:         "user-1",
		PlanID:         "plan-1",
		Plan: plan.Snapshot{
			Code:    "monthly",
			Cadence: 30 * 24 * time.Hour,
		},
		PeriodStart:  start,
		PeriodEnd:    start.Add(30 * 24 * time.Hour),
		RenewalCount: 0,
		Context:      Context{"k": "v"},
	}
}

func TestNextBillingPeriod_AdvancesByCadence(t *testing.T) {
	in := sample()
	next := NextBillingPeriod(in, "interval-next")

	if !next.PeriodStart.Equal(in.PeriodEnd) {
		t.Fatalf("PeriodStart = %v, want %v", next.PeriodStart, in.PeriodEnd)
	}
	if !next.PeriodEnd.Equal(in.PeriodEnd.Add(in.Plan.Cadence)) {
		t.Fatalf("PeriodEnd = %v, want %v", next.PeriodEnd, in.PeriodEnd.Add(in.Plan.Cadence))
	}
	if next.RenewalCount != in.RenewalCount+1 {
		t.Fatalf("RenewalCount = %d, want %d", next.RenewalCount, in.RenewalCount+1)
	}
	if next.CancelRequested {
		t.Fatalf("CancelRequested should reset to false")
	}
}

func TestNextBillingPeriod_PreservesIdentityFields(t *testing.T) {
	in := sample()
	in.Plan.PriceCents = 1234
	in.Plan.IntegrationEndpoint = "mock:50052"
	next := NextBillingPeriod(in, "interval-next")

	if next.UserID != in.UserID || next.PlanID != in.PlanID {
		t.Fatalf("identity lost")
	}
	if next.Plan.Code != in.Plan.Code || next.Plan.PriceCents != in.Plan.PriceCents {
		t.Fatalf("plan snapshot lost")
	}
	if next.Plan.IntegrationEndpoint != in.Plan.IntegrationEndpoint {
		t.Fatalf("integration endpoint lost")
	}
}

func TestNextBillingPeriod_ContextIsCloned(t *testing.T) {
	in := sample()
	next := NextBillingPeriod(in, "interval-next")
	next.Context["k"] = "mutated"
	if in.Context["k"] != "v" {
		t.Fatalf("mutating next leaked back into input")
	}
}
