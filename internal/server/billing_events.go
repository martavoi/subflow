package server

import (
	"context"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/billing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BillingEventsService struct {
	Events billing.EventStore
}

func (s *BillingEventsService) ListBillingEvents(ctx context.Context, req *subflowv1.ListBillingEventsRequest) (*subflowv1.ListBillingEventsResponse, error) {
	events, nextCursor, err := s.Events.List(ctx, billing.ListQuery{
		SubscriptionID: req.SubscriptionId,
		UserID:         req.UserId,
		TypeFilter:     req.TypeFilter,
		PageSize:       int(req.PageSize),
		PageCursor:     req.PageCursor,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list billing events: %v", err)
	}
	out := &subflowv1.ListBillingEventsResponse{
		Events:     make([]*subflowv1.BillingEvent, 0, len(events)),
		NextCursor: nextCursor,
	}
	for _, ev := range events {
		out.Events = append(out.Events, &subflowv1.BillingEvent{
			Id:             ev.ID,
			SubscriptionId: ev.SubscriptionID,
			UserId:         ev.UserID,
			PlanCode:       ev.PlanCode,
			Type:           ev.Type,
			AmountCents:    ev.AmountCents,
			Currency:       ev.Currency,
			PeriodStart:    timestamppb.New(ev.PeriodStart),
			PeriodEnd:      timestamppb.New(ev.PeriodEnd),
			RenewalCount:   int32(ev.RenewalCount),
			DunningAttempt: int32(ev.DunningAttempt),
			TransactionId:  ev.TransactionID,
			FailureReason:  ev.FailureReason,
			OccurredAt:     timestamppb.New(ev.OccurredAt),
		})
	}
	return out, nil
}
