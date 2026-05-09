package subscription

// NextBillingPeriod returns a SubscriptionInput for the period immediately
// following `current`. It is a pure function (no time.Now, no randomness)
// so it is safe to call from workflow code.
func NextBillingPeriod(current SubscriptionInput) SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  current.SubscriptionID,
		UserID:          current.UserID,
		PlanID:          current.PlanID,
		PlanCode:        current.PlanCode,
		BillingInterval: current.BillingInterval,
		IntegrationHost: current.IntegrationHost,
		PriceCents:      current.PriceCents,
		PeriodStart:     current.PeriodEnd,
		PeriodEnd:       current.PeriodEnd.Add(current.BillingInterval),
		Context:         current.Context.Clone(),
		RenewalCount:    current.RenewalCount + 1,
		CancelRequested: false,
	}
}
