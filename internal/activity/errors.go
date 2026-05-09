package activity

// Error type names used to classify failures. Names listed in a RetryPolicy's
// NonRetryableErrorTypes match against ApplicationError.Type values exactly.
const (
	ErrTypeInsufficientFunds   = "InsufficientFundsError"
	ErrTypeCardDeclined        = "CardDeclinedError"
	ErrTypeIntegrationTerminal = "IntegrationTerminalError"
)
