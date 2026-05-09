package server

import (
	"context"
	"errors"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PlanService implements the plan-related RPCs on SubflowService.
type PlanService struct {
	Repo *store.PlanRepository
}

func (s *PlanService) CreatePlan(ctx context.Context, req *subflowv1.CreatePlanRequest) (*subflowv1.Plan, error) {
	dur, err := time.ParseDuration(req.BillingInterval)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "billing_interval %q is not a valid Go duration: %v", req.BillingInterval, err)
	}
	p := plan.Plan{
		ID:                  uuid.NewString(),
		Code:                req.Code,
		Name:                req.Name,
		BillingInterval:     dur,
		PriceCents:          req.PriceCents,
		IntegrationEndpoint: req.IntegrationEndpoint,
		CreatedAt:           time.Now().UTC(),
	}
	if err := s.Repo.Insert(ctx, p); err != nil {
		return nil, status.Errorf(codes.Internal, "insert plan: %v", err)
	}
	return planToProto(p), nil
}

func (s *PlanService) GetPlan(ctx context.Context, req *subflowv1.GetPlanRequest) (*subflowv1.Plan, error) {
	p, err := s.Repo.Get(ctx, req.Id)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}
	return planToProto(p), nil
}

func (s *PlanService) ListPlans(ctx context.Context, _ *subflowv1.ListPlansRequest) (*subflowv1.ListPlansResponse, error) {
	plans, err := s.Repo.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list plans: %v", err)
	}
	out := &subflowv1.ListPlansResponse{Plans: make([]*subflowv1.Plan, 0, len(plans))}
	for _, p := range plans {
		out.Plans = append(out.Plans, planToProto(p))
	}
	return out, nil
}

func (s *PlanService) DeletePlan(ctx context.Context, req *subflowv1.DeletePlanRequest) (*subflowv1.DeletePlanResponse, error) {
	if err := s.Repo.Delete(ctx, req.Id); err != nil {
		if errors.Is(err, store.ErrPlanNotFound) {
			return nil, status.Error(codes.NotFound, "plan not found")
		}
		return nil, status.Errorf(codes.Internal, "delete plan: %v", err)
	}
	return &subflowv1.DeletePlanResponse{}, nil
}

func planToProto(p plan.Plan) *subflowv1.Plan {
	return &subflowv1.Plan{
		Id:                  p.ID,
		Code:                p.Code,
		Name:                p.Name,
		BillingInterval:     p.BillingInterval.String(),
		PriceCents:          p.PriceCents,
		IntegrationEndpoint: p.IntegrationEndpoint,
	}
}
