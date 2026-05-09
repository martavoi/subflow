package activity

import (
	"context"
	"log/slog"
	"math/rand/v2"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ChargePaymentInput is the activity input. Reference is the idempotency token
// constructed by the workflow.
type ChargePaymentInput struct {
	Reference  string
	UserID     string
	PriceCents int64
}

type ChargePaymentResult struct {
	Reference     string
	TransactionID string
	AmountCents   int64
}

// PaymentActivities holds the (mocked) configuration for charging payments.
// In a real implementation this would hold a payment gateway client.
type PaymentActivities struct {
	TransientFailureRate float64
	TerminalFailureRate  float64
}

// ChargePayment is the registered activity. It simulates a payment charge
// with configurable failure injection.
func (a *PaymentActivities) ChargePayment(ctx context.Context, in ChargePaymentInput) (ChargePaymentResult, error) {
	logger := activity.GetLogger(ctx)

	r := rand.Float64()
	switch {
	case r < a.TerminalFailureRate:
		logger.Warn("ChargePayment terminal failure (declined)", slog.String("ref", in.Reference))
		return ChargePaymentResult{}, temporal.NewNonRetryableApplicationError(
			"card declined", ErrTypeCardDeclined, nil)
	case r < a.TerminalFailureRate+a.TransientFailureRate:
		logger.Warn("ChargePayment transient failure", slog.String("ref", in.Reference))
		return ChargePaymentResult{}, temporal.NewApplicationError(
			"payment gateway timeout", "PaymentGatewayTimeoutError")
	}

	logger.Info("ChargePayment success",
		slog.String("ref", in.Reference),
		slog.String("user", in.UserID),
		slog.Int64("cents", in.PriceCents))

	return ChargePaymentResult{
		Reference:     in.Reference,
		TransactionID: "txn-" + in.Reference,
		AmountCents:   in.PriceCents,
	}, nil
}
