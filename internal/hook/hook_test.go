package hook_test

import (
	"testing"

	"github.com/martavoi/subflow/internal/hook"
)

func TestParse_ValidNames(t *testing.T) {
	cases := []struct {
		wire string
		want hook.Type
	}{
		{"subscription.trial_started", hook.TrialStarted},
		{"subscription.trial_will_end", hook.TrialWillEnd},
		{"subscription.renewal_upcoming", hook.RenewalUpcoming},
		{"subscription.activated", hook.Activated},
		{"subscription.renewed", hook.Renewed},
		{"subscription.past_due", hook.PastDue},
		{"subscription.recovered", hook.Recovered},
		{"subscription.canceled", hook.Canceled},
		{"subscription.deactivated", hook.Deactivated},
		{"payment.succeeded", hook.PaymentOK},
		{"payment.failed", hook.PaymentFailed},
	}
	for _, tc := range cases {
		t.Run(tc.wire, func(t *testing.T) {
			got, err := hook.Parse(tc.wire)
			if err != nil {
				t.Fatalf("Parse(%q) returned unexpected error: %v", tc.wire, err)
			}
			if got != tc.want {
				t.Fatalf("Parse(%q) = %q, want %q", tc.wire, got, tc.want)
			}
		})
	}
}

func TestParse_Rejection(t *testing.T) {
	rejects := []string{
		"",
		"unknown",
		"subscription.unknown",
		"trial_started",          // missing "subscription." prefix
		"SUBSCRIPTION.ACTIVATED", // case-sensitive
	}
	for _, s := range rejects {
		t.Run(s, func(t *testing.T) {
			_, err := hook.Parse(s)
			if err == nil {
				t.Fatalf("Parse(%q) expected error, got nil", s)
			}
		})
	}
}

func TestAll_ContainsAllConstants(t *testing.T) {
	// Verify that All contains exactly the 11 declared constants and no
	// duplicates. This test will fail if a new constant is added without
	// being added to All.
	expected := map[hook.Type]bool{
		hook.TrialStarted:    true,
		hook.TrialWillEnd:    true,
		hook.RenewalUpcoming: true,
		hook.Activated:       true,
		hook.Renewed:         true,
		hook.PastDue:         true,
		hook.Recovered:       true,
		hook.Canceled:        true,
		hook.Deactivated:     true,
		hook.PaymentOK:       true,
		hook.PaymentFailed:   true,
	}
	if len(hook.All) != len(expected) {
		t.Fatalf("hook.All has %d entries, expected %d", len(hook.All), len(expected))
	}
	seen := make(map[hook.Type]bool, len(hook.All))
	for _, h := range hook.All {
		if !expected[h] {
			t.Errorf("hook.All contains unexpected entry %q", h)
		}
		if seen[h] {
			t.Errorf("hook.All contains duplicate entry %q", h)
		}
		seen[h] = true
	}
}

// Compile-time check that both payload types satisfy the sealed Payload
// interface. If a payload type stops satisfying it, this file won't compile.
var (
	_ hook.Payload = hook.LifecyclePayload{}
	_ hook.Payload = hook.PaymentPayload{}
)
