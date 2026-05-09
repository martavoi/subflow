package activity

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// PaymentRetry retries transient payment failures with backoff but stops on
// known terminal billing errors (declined, insufficient funds).
var PaymentRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    5,
	NonRetryableErrorTypes: []string{
		ErrTypeInsufficientFunds,
		ErrTypeCardDeclined,
	},
}

// EventPublishingRetry retries forever — events should eventually publish.
// Operator can fix the bus and let it drain.
var EventPublishingRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 1.5,
	MaximumInterval:    30 * time.Second,
	MaximumAttempts:    0,
}

// IntegrationCallRetry retries forever for transient gRPC failures and stops
// only on integration-side terminal errors.
var IntegrationCallRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    0,
	NonRetryableErrorTypes: []string{
		ErrTypeIntegrationTerminal,
	},
}
