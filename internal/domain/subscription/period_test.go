package subscription

import (
	"testing"
	"time"
)

func TestNextBillingPeriod_AdvancesByBillingInterval(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := SubscriptionInput{
		SubscriptionID:  "sub-1",
		PlanID:          "plan-1",
		BillingInterval: 30 * 24 * time.Hour,
		PeriodStart:     start,
		PeriodEnd:       start.Add(30 * 24 * time.Hour),
		RenewalCount:    0,
		Context:         Context{"k": "v"},
	}

	next := NextBillingPeriod(in)

	if got, want := next.PeriodStart, in.PeriodEnd; !got.Equal(want) {
		t.Fatalf("PeriodStart = %v, want %v", got, want)
	}
	if got, want := next.PeriodEnd, in.PeriodEnd.Add(in.BillingInterval); !got.Equal(want) {
		t.Fatalf("PeriodEnd = %v, want %v", got, want)
	}
	if got, want := next.RenewalCount, in.RenewalCount+1; got != want {
		t.Fatalf("RenewalCount = %d, want %d", got, want)
	}
	if next.CancelRequested {
		t.Fatalf("CancelRequested should never carry forward into next period")
	}
	if got, want := next.SubscriptionID, in.SubscriptionID; got != want {
		t.Fatalf("SubscriptionID = %q, want %q", got, want)
	}
}

func TestNextBillingPeriod_PreservesIdentityFields(t *testing.T) {
	in := SubscriptionInput{
		SubscriptionID:  "sub-2",
		UserID:          "user-1",
		PlanID:          "plan-1",
		PlanCode:        "monthly",
		BillingInterval: time.Hour,
		IntegrationHost: "mock:50052",
		PriceCents:      999,
		PeriodStart:     time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:       time.Date(2026, 5, 1, 1, 0, 0, 0, time.UTC),
		Context:         Context{"card_id": "card_001"},
		RenewalCount:    3,
	}

	next := NextBillingPeriod(in)

	if next.UserID != in.UserID {
		t.Fatalf("UserID lost across period roll")
	}
	if next.PlanID != in.PlanID || next.PlanCode != in.PlanCode {
		t.Fatalf("Plan identifiers lost across period roll")
	}
	if next.IntegrationHost != in.IntegrationHost {
		t.Fatalf("IntegrationHost lost across period roll")
	}
	if next.PriceCents != in.PriceCents {
		t.Fatalf("PriceCents lost across period roll")
	}
	if got, want := next.Context["card_id"], in.Context["card_id"]; got != want {
		t.Fatalf("Context lost: got %q, want %q", got, want)
	}
}

func TestNextBillingPeriod_ContextIsCloned(t *testing.T) {
	in := SubscriptionInput{
		BillingInterval: time.Hour,
		PeriodEnd:       time.Now().Add(time.Hour),
		Context:         Context{"k": "v"},
	}

	next := NextBillingPeriod(in)
	next.Context["k"] = "mutated"

	if in.Context["k"] != "v" {
		t.Fatalf("mutating next.Context leaked back to input.Context")
	}
}
