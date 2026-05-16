package activity

import (
	"context"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/hook"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DispatchHook is the single activity input for all hook dispatches.
type DispatchHook struct {
	Endpoint       string
	EventID        string
	Type           hook.Type
	SubscriptionID string
	UserID         string
	PlanCode       string
	RenewalCount   int
	EventTime      time.Time
	Context        map[string]string

	// Exactly one of these is populated based on hook category.
	Lifecycle *LifecycleData
	Payment   *PaymentData
}

// LifecycleData carries payload fields for lifecycle hooks (8 types).
type LifecycleData struct {
	Phase       string
	PeriodStart time.Time
	PeriodEnd   time.Time
}

// PaymentData carries payload fields for payment hooks (2 types).
type PaymentData struct {
	DunningAttempt int
	AmountCents    int64
	Currency       string
	TransactionID  string
	FailureReason  string
}

// HookDispatcher groups the hook dispatch activity. A single Dispatch method
// replaces the 10 per-hook On* methods.
type HookDispatcher struct {
	Client *integration.Client
}

// Dispatch is the single registered activity for all hook types. It builds a
// proto Event with the appropriate oneof payload and calls the integration's
// Dispatch rpc.
func (h *HookDispatcher) Dispatch(ctx context.Context, in DispatchHook) error {
	ev := &subflowv1.Event{
		Id:             in.EventID,
		Type:           string(in.Type),
		CreatedAt:      timestamppb.New(in.EventTime),
		Context:        in.Context,
		SubscriptionId: in.SubscriptionID,
		UserId:         in.UserID,
		PlanCode:       in.PlanCode,
		RenewalCount:   int32(in.RenewalCount),
	}

	switch {
	case in.Lifecycle != nil:
		ev.Data = &subflowv1.Event_Lifecycle{
			Lifecycle: &subflowv1.LifecycleData{
				Phase:       in.Lifecycle.Phase,
				PeriodStart: timestamppb.New(in.Lifecycle.PeriodStart),
				PeriodEnd:   timestamppb.New(in.Lifecycle.PeriodEnd),
			},
		}
	case in.Payment != nil:
		ev.Data = &subflowv1.Event_Payment{
			Payment: &subflowv1.PaymentData{
				DunningAttempt: int32(in.Payment.DunningAttempt),
				AmountCents:    in.Payment.AmountCents,
				Currency:       in.Payment.Currency,
				TransactionId:  in.Payment.TransactionID,
				FailureReason:  in.Payment.FailureReason,
			},
		}
	}

	return mapHookError(h.Client.Dispatch(ctx, in.Endpoint, ev))
}

// mapHookError converts gRPC errors to Temporal application errors. Terminal
// codes (FailedPrecondition / InvalidArgument / NotFound / Unimplemented) become
// non-retryable HookTerminalError; everything else stays retryable.
func mapHookError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.FailedPrecondition, codes.InvalidArgument, codes.NotFound, codes.Unimplemented:
			return temporal.NewNonRetryableApplicationError(st.Message(), ErrTypeHookTerminal, err)
		}
	}
	return temporal.NewApplicationError(err.Error(), "HookTransientError")
}
