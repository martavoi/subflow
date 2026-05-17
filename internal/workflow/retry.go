package workflow

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// ChargePaymentRetry handles transient payment-gateway failures. Stops on
// terminal billing errors (declined, insufficient funds).
var ChargePaymentRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    5,
	NonRetryableErrorTypes: []string{
		ErrTypeInsufficientFunds,
		ErrTypeCardDeclined,
	},
}

// BillingEventRetry is bounded — if Mongo is down for 20 minutes, we give up
// at the activity boundary. The charge already happened; the workflow logs
// a critical error and proceeds. Workflow history is the forensic fallback.
var BillingEventRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    20,
}

// HookRetry handles transient integration failures. Unlimited retries —
// hooks must eventually deliver. Integrators return HookTerminalError to
// permanently fail.
var HookRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    5 * time.Minute,
	MaximumAttempts:    0,
	NonRetryableErrorTypes: []string{
		ErrTypeHookTerminal,
	},
}
