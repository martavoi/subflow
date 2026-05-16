package subscription

// NextBillingPeriod returns a SubscriptionInput for the period immediately
// following `current`. Pure function (no time.Now, no randomness — the caller
// supplies the fresh IntervalID since UUID generation is non-deterministic
// and belongs at the workflow boundary via workflow.SideEffect).
func NextBillingPeriod(current SubscriptionInput, nextIntervalID string) SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  current.SubscriptionID,
		IntervalID:      nextIntervalID,
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
