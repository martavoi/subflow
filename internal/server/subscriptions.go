package server

import (
	"context"
	"errors"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/store"
	"github.com/martavoi/subflow/internal/workflow"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SubscriptionService translates subscription RPCs into Temporal client calls
// + projection lookups. Contains zero business logic.
type SubscriptionService struct {
	Temporal      client.Client
	TaskQueue     string
	PlanRepo      *store.PlanRepository
	Projection    *store.SubscriptionProjectionRepository
	DefaultIntegration string
}

func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *subflowv1.CreateSubscriptionRequest) (*subflowv1.Subscription, error) {
	p, err := s.PlanRepo.Get(ctx, req.PlanId)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}

	now := time.Now().UTC()
	subID := uuid.NewString()
	wfInput := subscription.SubscriptionInput{
		SubscriptionID:  subID,
		UserID:          req.UserId,
		PlanID:          p.ID,
		PlanCode:        p.Code,
		BillingInterval: p.BillingInterval,
		IntegrationHost: integrationFor(p, s.DefaultIntegration),
		PriceCents:      p.PriceCents,
		PeriodStart:     now,
		PeriodEnd:       now.Add(p.BillingInterval),
		Context:         subscription.Context(req.InitialContext),
	}

	_, err = s.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "subscription:" + subID,
		TaskQueue: s.TaskQueue,
	}, workflow.SubscriptionWorkflow, wfInput)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start workflow: %v", err)
	}

	// Optimistic projection so List works immediately (worker will overwrite as it progresses).
	_ = s.Projection.Upsert(ctx, store.SubscriptionView{
		ID:           subID,
		UserID:       req.UserId,
		PlanID:       p.ID,
		Phase:        activity.PhasePending,
		PeriodStart:  wfInput.PeriodStart,
		PeriodEnd:    wfInput.PeriodEnd,
		RenewalCount: 0,
		Context:      wfInput.Context,
	})

	return &subflowv1.Subscription{
		Id:           subID,
		UserId:       req.UserId,
		PlanId:       p.ID,
		Phase:        activity.PhasePending,
		PeriodStart:  timestamppb.New(wfInput.PeriodStart),
		PeriodEnd:    timestamppb.New(wfInput.PeriodEnd),
		RenewalCount: 0,
		Context:      map[string]string(wfInput.Context),
	}, nil
}

func (s *SubscriptionService) CancelSubscription(ctx context.Context, req *subflowv1.CancelSubscriptionRequest) (*subflowv1.CancelSubscriptionResponse, error) {
	err := s.Temporal.SignalWorkflow(ctx, "subscription:"+req.Id, "", workflow.SignalCancelSubscription, nil)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "signal workflow: %v", err)
	}
	return &subflowv1.CancelSubscriptionResponse{}, nil
}

func (s *SubscriptionService) GetSubscription(ctx context.Context, req *subflowv1.GetSubscriptionRequest) (*subflowv1.Subscription, error) {
	// Try query first (live state from Temporal).
	res, err := s.Temporal.QueryWorkflow(ctx, "subscription:"+req.Id, "", workflow.QuerySubscriptionStatus)
	if err == nil {
		var st workflow.SubscriptionStatus
		if err := res.Get(&st); err == nil {
			view, _ := s.Projection.Get(ctx, req.Id)
			return statusToProto(req.Id, view, st), nil
		}
	}

	// Fall back to projection (workflow may have completed).
	view, err := s.Projection.Get(ctx, req.Id)
	if errors.Is(err, store.ErrSubscriptionNotFound) {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get projection: %v", err)
	}
	return viewToProto(view), nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, req *subflowv1.ListSubscriptionsRequest) (*subflowv1.ListSubscriptionsResponse, error) {
	views, err := s.Projection.List(ctx, req.UserId, req.Phase)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	out := &subflowv1.ListSubscriptionsResponse{Subscriptions: make([]*subflowv1.Subscription, 0, len(views))}
	for _, v := range views {
		out.Subscriptions = append(out.Subscriptions, viewToProto(v))
	}
	return out, nil
}

func integrationFor(p plan.Plan, fallback string) string {
	if p.IntegrationEndpoint != "" {
		return p.IntegrationEndpoint
	}
	return fallback
}

func statusToProto(id string, view store.SubscriptionView, st workflow.SubscriptionStatus) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:              id,
		UserId:          view.UserID,
		PlanId:          view.PlanID,
		Phase:           st.Phase,
		PeriodStart:     timestamppb.New(st.PeriodStart),
		PeriodEnd:       timestamppb.New(st.PeriodEnd),
		RenewalCount:    int32(st.RenewalCount),
		Context:         map[string]string(st.Context),
		CancelRequested: st.CancelRequested,
	}
}

func viewToProto(v store.SubscriptionView) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:              v.ID,
		UserId:          v.UserID,
		PlanId:          v.PlanID,
		Phase:           v.Phase,
		PeriodStart:     timestamppb.New(v.PeriodStart),
		PeriodEnd:       timestamppb.New(v.PeriodEnd),
		RenewalCount:    int32(v.RenewalCount),
		Context:         map[string]string(v.Context),
		CancelRequested: v.CancelRequested,
	}
}
