package hook_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/martavoi/subflow/internal/hook"
)

func TestEvent_JSONRoundtrip_Lifecycle(t *testing.T) {
	t1 := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(30 * 24 * time.Hour)
	want := hook.Event{
		Type:           hook.Activated,
		SubscriptionID: "sub-1",
		UserID:         "user-1",
		PlanCode:       "monthly-basic",
		RenewalCount:   3,
		OccurredAt:     t1,
		Context:        map[string]string{"card_id": "card_001"},
		Payload: hook.LifecyclePayload{
			Phase:       "active",
			PeriodStart: t1,
			PeriodEnd:   t2,
		},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got hook.Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if got.Type != want.Type {
		t.Errorf("Type: got %q, want %q", got.Type, want.Type)
	}
	if got.SubscriptionID != want.SubscriptionID {
		t.Errorf("SubscriptionID: got %q, want %q", got.SubscriptionID, want.SubscriptionID)
	}
	if got.UserID != want.UserID {
		t.Errorf("UserID: got %q, want %q", got.UserID, want.UserID)
	}
	if got.PlanCode != want.PlanCode {
		t.Errorf("PlanCode: got %q, want %q", got.PlanCode, want.PlanCode)
	}
	if got.RenewalCount != want.RenewalCount {
		t.Errorf("RenewalCount: got %d, want %d", got.RenewalCount, want.RenewalCount)
	}
	if !got.OccurredAt.Equal(want.OccurredAt) {
		t.Errorf("OccurredAt: got %v, want %v", got.OccurredAt, want.OccurredAt)
	}
	if got.Context["card_id"] != "card_001" {
		t.Errorf("Context: got %v, want card_id=card_001", got.Context)
	}
	gotLP, ok := got.Payload.(hook.LifecyclePayload)
	if !ok {
		t.Fatalf("Payload: got %T, want hook.LifecyclePayload", got.Payload)
	}
	if gotLP.Phase != "active" {
		t.Errorf("Payload.Phase: got %q, want active", gotLP.Phase)
	}
	if !gotLP.PeriodStart.Equal(t1) {
		t.Errorf("Payload.PeriodStart: got %v, want %v", gotLP.PeriodStart, t1)
	}
	if !gotLP.PeriodEnd.Equal(t2) {
		t.Errorf("Payload.PeriodEnd: got %v, want %v", gotLP.PeriodEnd, t2)
	}
}

func TestEvent_JSONRoundtrip_Payment(t *testing.T) {
	t1 := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	want := hook.Event{
		Type:           hook.PaymentOK,
		SubscriptionID: "sub-2",
		UserID:         "user-2",
		PlanCode:       "monthly-basic",
		RenewalCount:   5,
		OccurredAt:     t1,
		Payload: hook.PaymentPayload{
			DunningAttempt: 1,
			AmountCents:    999,
			Currency:       "USD",
			TransactionID:  "txn-abc",
		},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got hook.Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	gotPP, ok := got.Payload.(hook.PaymentPayload)
	if !ok {
		t.Fatalf("Payload: got %T, want hook.PaymentPayload", got.Payload)
	}
	if gotPP.DunningAttempt != 1 {
		t.Errorf("DunningAttempt: got %d, want 1", gotPP.DunningAttempt)
	}
	if gotPP.AmountCents != 999 {
		t.Errorf("AmountCents: got %d, want 999", gotPP.AmountCents)
	}
	if gotPP.Currency != "USD" {
		t.Errorf("Currency: got %q, want USD", gotPP.Currency)
	}
	if gotPP.TransactionID != "txn-abc" {
		t.Errorf("TransactionID: got %q, want txn-abc", gotPP.TransactionID)
	}
}

func TestEvent_MarshalJSON_NilPayloadErrors(t *testing.T) {
	e := hook.Event{Type: hook.Activated}
	_, err := json.Marshal(e)
	if err == nil {
		t.Fatal("expected error marshaling Event with nil Payload, got nil")
	}
	if !strings.Contains(err.Error(), "unknown payload type") {
		t.Errorf("error message should mention unknown payload type, got: %v", err)
	}
}

func TestEvent_UnmarshalJSON_UnknownKindErrors(t *testing.T) {
	raw := `{"type":"subscription.activated","subscription_id":"s","user_id":"u","plan_code":"p","renewal_count":0,"occurred_at":"2026-05-17T12:00:00Z","payload_kind":"bogus","payload_data":{}}`
	var got hook.Event
	err := json.Unmarshal([]byte(raw), &got)
	if err == nil {
		t.Fatal("expected error unmarshaling unknown payload_kind, got nil")
	}
	if !strings.Contains(err.Error(), "unknown payload_kind") {
		t.Errorf("error message should mention unknown payload_kind, got: %v", err)
	}
}
