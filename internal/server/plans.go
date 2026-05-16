package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/hook"
	"github.com/martavoi/subflow/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PlanService struct {
	Repo *store.PlanRepository
}

func (s *PlanService) CreatePlan(ctx context.Context, req *subflowv1.CreatePlanRequest) (*subflowv1.Plan, error) {
	p, err := buildPlanFromRequest(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
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

func buildPlanFromRequest(req *subflowv1.CreatePlanRequest) (plan.Plan, error) {
	cadence, err := time.ParseDuration(req.Cadence)
	if err != nil || cadence <= 0 {
		return plan.Plan{}, fmt.Errorf("cadence %q invalid (must be positive Go duration)", req.Cadence)
	}
	trial, err := parseOptionalDuration(req.TrialDuration)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("trial_duration: %w", err)
	}
	notice, err := parseOptionalDuration(req.TrialEndNoticeBefore)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("trial_end_notice_before: %w", err)
	}
	if notice > 0 && trial > 0 && notice >= trial {
		return plan.Plan{}, fmt.Errorf("trial_end_notice_before (%s) must be less than trial_duration (%s)", notice, trial)
	}
	backoff, err := parseOptionalDuration(req.DunningRetryBackoff)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("dunning_retry_backoff: %w", err)
	}
	if req.DunningMaxAttempts < 0 || req.DunningMaxAttempts > 99 {
		return plan.Plan{}, fmt.Errorf("dunning_max_attempts %d out of range [0,99]", req.DunningMaxAttempts)
	}
	if req.PriceCents < 0 {
		return plan.Plan{}, fmt.Errorf("price_cents must be >= 0")
	}
	if req.PerUserLimit < 0 {
		return plan.Plan{}, fmt.Errorf("per_user_limit must be >= 0 (0 = unlimited)")
	}
	hooks := make([]hook.Type, 0, len(req.EnabledHooks))
	for _, raw := range req.EnabledHooks {
		h, err := hook.Parse(raw)
		if err != nil {
			return plan.Plan{}, err
		}
		hooks = append(hooks, h)
	}

	currency := strings.ToUpper(req.Currency)
	if currency == "" {
		currency = "USD"
	}
	dunningMaxAttempts := int(req.DunningMaxAttempts)
	if dunningMaxAttempts == 0 {
		dunningMaxAttempts = 3
	}
	if backoff == 0 {
		backoff = 24 * time.Hour
	}

	return plan.Plan{
		ID:                   uuid.NewString(),
		Code:                 req.Code,
		Name:                 req.Name,
		Cadence:              cadence,
		PriceCents:           req.PriceCents,
		Currency:             currency,
		PerUserLimit:         int(req.PerUserLimit),
		TrialDuration:        trial,
		TrialEndNoticeBefore: notice,
		DunningMaxAttempts:   dunningMaxAttempts,
		DunningRetryBackoff:  backoff,
		IntegrationEndpoint:  req.IntegrationEndpoint,
		EnabledHooks:         hooks,
		CreatedAt:            time.Now().UTC(),
	}, nil
}

func parseOptionalDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func planToProto(p plan.Plan) *subflowv1.Plan {
	hookStrings := make([]string, len(p.EnabledHooks))
	for i, h := range p.EnabledHooks {
		hookStrings[i] = string(h)
	}
	return &subflowv1.Plan{
		Id:                   p.ID,
		Code:                 p.Code,
		Name:                 p.Name,
		Cadence:              p.Cadence.String(),
		PriceCents:           p.PriceCents,
		Currency:             p.Currency,
		PerUserLimit:         int32(p.PerUserLimit),
		TrialDuration:        durationOrEmpty(p.TrialDuration),
		TrialEndNoticeBefore: durationOrEmpty(p.TrialEndNoticeBefore),
		DunningMaxAttempts:   int32(p.DunningMaxAttempts),
		DunningRetryBackoff:  durationOrEmpty(p.DunningRetryBackoff),
		IntegrationEndpoint:  p.IntegrationEndpoint,
		EnabledHooks:         hookStrings,
	}
}

func durationOrEmpty(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}
