package workflow

// Application error types used by activities. Names appear in RetryPolicy
// NonRetryableErrorTypes lists, matched against ApplicationError.Type exactly.
const (
	// Payment terminal failures — never retry.
	ErrTypeInsufficientFunds = "InsufficientFundsError"
	ErrTypeCardDeclined      = "CardDeclinedError"

	// Payment transient — retried by Temporal per ChargePaymentRetry.
	ErrTypePaymentGatewayTimeout = "PaymentGatewayTimeoutError"

	// Integration hook terminal — never retry; integrator explicitly rejected.
	ErrTypeHookTerminal = "HookTerminalError"

	// Billing event store gave up — workflow logs and continues.
	ErrTypeBillingStoreExhausted = "BillingStoreExhaustedError"
)
