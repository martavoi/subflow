package activity

import (
	"context"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// LifecycleHookInput is the activity input for the 8 subscription-level hooks.
type LifecycleHookInput struct {
	Reference           string
	IntegrationEndpoint string
	HookName            string
	SubscriptionID      string
	UserID              string
	PlanCode            string
	Phase               string
	RenewalCount        int
	PeriodStart         time.Time
	PeriodEnd           time.Time
	EventTime           time.Time
	Context             map[string]string
}

// PaymentHookInput is the activity input for the 2 payment-level hooks.
type PaymentHookInput struct {
	Reference           string
	IntegrationEndpoint string
	HookName            string
	SubscriptionID      string
	UserID              string
	PlanCode            string
	RenewalCount        int
	DunningAttempt      int
	AmountCents         int64
	Currency            string
	TransactionID       string
	FailureReason       string
	EventTime           time.Time
	Context             map[string]string
}

// HookActivities groups the 10 hook dispatch activities, all of which share
// the same integration client.
type HookActivities struct {
	Client *integration.Client
}

func (a *HookActivities) dispatchLifecycle(ctx context.Context, in LifecycleHookInput) error {
	ev := &subflowv1.LifecycleEvent{
		Reference:      in.Reference,
		SubscriptionId: in.SubscriptionID,
		UserId:         in.UserID,
		PlanCode:       in.PlanCode,
		Phase:          in.Phase,
		RenewalCount:   int32(in.RenewalCount),
		PeriodStart:    timestamppb.New(in.PeriodStart),
		PeriodEnd:      timestamppb.New(in.PeriodEnd),
		EventTime:      timestamppb.New(in.EventTime),
		Context:        in.Context,
	}
	return mapHookError(a.Client.DispatchLifecycle(ctx, in.IntegrationEndpoint, in.HookName, ev))
}

func (a *HookActivities) dispatchPayment(ctx context.Context, in PaymentHookInput) error {
	ev := &subflowv1.PaymentEvent{
		Reference:      in.Reference,
		SubscriptionId: in.SubscriptionID,
		UserId:         in.UserID,
		PlanCode:       in.PlanCode,
		RenewalCount:   int32(in.RenewalCount),
		DunningAttempt: int32(in.DunningAttempt),
		AmountCents:    in.AmountCents,
		Currency:       in.Currency,
		TransactionId:  in.TransactionID,
		FailureReason:  in.FailureReason,
		EventTime:      timestamppb.New(in.EventTime),
		Context:        in.Context,
	}
	return mapHookError(a.Client.DispatchPayment(ctx, in.IntegrationEndpoint, in.HookName, ev))
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

// 10 registered methods — each is a thin wrapper. We keep them as discrete
// registered names so the Web UI shows the hook name on every activity execution.

func (a *HookActivities) OnTrialStarted(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnTrialWillEnd(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnActivated(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnRenewed(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnPastDue(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnRecovered(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnCanceled(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnDeactivated(ctx context.Context, in LifecycleHookInput) error {
	return a.dispatchLifecycle(ctx, in)
}

func (a *HookActivities) OnPaymentSucceeded(ctx context.Context, in PaymentHookInput) error {
	return a.dispatchPayment(ctx, in)
}

func (a *HookActivities) OnPaymentFailed(ctx context.Context, in PaymentHookInput) error {
	return a.dispatchPayment(ctx, in)
}
