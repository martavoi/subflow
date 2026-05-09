package workflow

// Signal and query names for SubscriptionWorkflow. Stable strings — clients
// reference these directly.
const (
	SignalCancelSubscription = "subscription.cancel"
	QuerySubscriptionStatus  = "subscription.status"
)
