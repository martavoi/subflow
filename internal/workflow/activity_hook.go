package workflow

import (
	"context"
	"fmt"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/hook"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DispatchHook is the activity input for delivering an integration event to
// the integrator. Wraps the canonical hook.Event with delivery metadata
// (Endpoint, EventID).
type DispatchHook struct {
	Event    hook.Event
	Endpoint string
	EventID  string // idempotency key
}

// HookDispatcher groups the hook dispatch activity. A single Dispatch method
// handles every payload variant via a type switch on Event.Payload.
type HookDispatcher struct {
	Client *integration.Client
}

// Dispatch is the single registered activity for all hook types. It builds
// a proto Event with the appropriate oneof payload variant and calls the
// integration's Dispatch rpc.
func (h *HookDispatcher) Dispatch(ctx context.Context, in DispatchHook) error {
	ev := &subflowv1.Event{
		Id:             in.EventID,
		Type:           string(in.Event.Type),
		CreatedAt:      timestamppb.New(in.Event.OccurredAt),
		Context:        in.Event.Context,
		SubscriptionId: in.Event.SubscriptionID,
		UserId:         in.Event.UserID,
		PlanCode:       in.Event.PlanCode,
		RenewalCount:   int32(in.Event.RenewalCount),
	}

	switch p := in.Event.Payload.(type) {
	case hook.LifecyclePayload:
		ev.Data = &subflowv1.Event_Lifecycle{
			Lifecycle: &subflowv1.LifecycleData{
				Phase:       p.Phase,
				PeriodStart: timestamppb.New(p.PeriodStart),
				PeriodEnd:   timestamppb.New(p.PeriodEnd),
			},
		}
	case hook.PaymentPayload:
		ev.Data = &subflowv1.Event_Payment{
			Payment: &subflowv1.PaymentData{
				DunningAttempt: int32(p.DunningAttempt),
				AmountCents:    p.AmountCents,
				Currency:       p.Currency,
				TransactionId:  p.TransactionID,
				FailureReason:  p.FailureReason,
			},
		}
	default:
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("DispatchHook: unknown payload type %T", in.Event.Payload),
			ErrTypeHookTerminal, nil)
	}

	return mapHookError(h.Client.Dispatch(ctx, in.Endpoint, ev))
}

// mapHookError converts gRPC errors to Temporal application errors. Terminal
// codes become non-retryable HookTerminalError; everything else stays
// retryable.
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
