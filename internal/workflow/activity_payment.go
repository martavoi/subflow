package workflow

import (
	"context"
	"log/slog"
	"math/rand/v2"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type ChargePayment struct {
	Reference   string
	UserID      string
	PlanCode    string
	AmountCents int64
	Currency    string
}

type ChargeResult struct {
	Reference     string
	TransactionID string
	AmountCents   int64
	Currency      string
}

// PaymentGateway is the mocked payment gateway. In production this would
// hold a real Stripe/Adyen/etc. client; here we inject failures probabilistically.
type PaymentGateway struct {
	TransientFailureRate float64
	TerminalFailureRate  float64
}

// ChargePayment simulates a charge attempt. Returns terminal or transient
// errors based on configured rates.
func (a *PaymentGateway) ChargePayment(ctx context.Context, in ChargePayment) (ChargeResult, error) {
	logger := activity.GetLogger(ctx)

	r := rand.Float64()
	switch {
	case r < a.TerminalFailureRate:
		logger.Warn("ChargePayment terminal (declined)", slog.String("ref", in.Reference))
		return ChargeResult{}, temporal.NewNonRetryableApplicationError(
			"card declined", ErrTypeCardDeclined, nil)
	case r < a.TerminalFailureRate+a.TransientFailureRate:
		logger.Warn("ChargePayment transient gateway timeout", slog.String("ref", in.Reference))
		return ChargeResult{}, temporal.NewApplicationError(
			"payment gateway timeout", ErrTypePaymentGatewayTimeout)
	}

	logger.Info("ChargePayment success",
		slog.String("ref", in.Reference),
		slog.String("user", in.UserID),
		slog.String("plan", in.PlanCode),
		slog.Int64("cents", in.AmountCents),
		slog.String("currency", in.Currency))

	return ChargeResult{
		Reference:     in.Reference,
		TransactionID: "txn-" + in.Reference,
		AmountCents:   in.AmountCents,
		Currency:      in.Currency,
	}, nil
}
