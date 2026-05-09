package activity

import (
	"context"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IntegrationCallInput struct {
	Reference       string
	EventType       string
	UserID          string
	PlanCode        string
	IntegrationHost string
	Context         subscription.Context
}

type IntegrationCallResult struct {
	UpdatedContext subscription.Context
}

type IntegrationActivities struct {
	Client *integration.Client
}

func (a *IntegrationActivities) NotifyIntegrationService(ctx context.Context, in IntegrationCallInput) (IntegrationCallResult, error) {
	resp, err := a.Client.HandleEvent(ctx, in.IntegrationHost, &subflowv1.IntegrationEvent{
		Reference: in.Reference,
		EventType: in.EventType,
		UserId:    in.UserID,
		PlanCode:  in.PlanCode,
		Context:   map[string]string(in.Context),
	})
	if err != nil {
		// Map gRPC status codes to Temporal error semantics.
		// FailedPrecondition / InvalidArgument / NotFound -> non-retryable terminal.
		// Everything else (Unavailable, DeadlineExceeded, Unknown) -> retryable.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.FailedPrecondition, codes.InvalidArgument, codes.NotFound:
				return IntegrationCallResult{}, temporal.NewNonRetryableApplicationError(
					st.Message(), ErrTypeIntegrationTerminal, err)
			}
		}
		return IntegrationCallResult{}, temporal.NewApplicationError(
			err.Error(), "IntegrationTransientError")
	}

	return IntegrationCallResult{
		UpdatedContext: subscription.Context(resp.UpdatedContext),
	}, nil
}
