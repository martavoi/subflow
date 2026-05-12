package workflow

// Signal, query, and update names exchanged with the SubscriptionWorkflow.
const (
	SignalCancelSubscription = "subscription.cancel"
	SignalUpdateContext      = "subscription.update_context"
	QuerySubscriptionStatus  = "subscription.status"
	UpdateActivate           = "subscription.activate"
)
