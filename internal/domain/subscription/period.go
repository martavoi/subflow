package subscription

// NextBillingPeriod returns a SubscriptionInput for the period immediately
// following `current`. Pure function (no time.Now, no randomness).
func NextBillingPeriod(current SubscriptionInput) SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  current.SubscriptionID,
		UserID:          current.UserID,
		PlanID:          current.PlanID,
		Plan:            current.Plan,
		PeriodStart:     current.PeriodEnd,
		PeriodEnd:       current.PeriodEnd.Add(current.Plan.Cadence),
		Context:         current.Context.Clone(),
		RenewalCount:    current.RenewalCount + 1,
		CancelRequested: false,
	}
}
