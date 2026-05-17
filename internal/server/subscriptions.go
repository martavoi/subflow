package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/store"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"github.com/martavoi/subflow/internal/workflow"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SubscriptionService struct {
	Temporal  client.Client
	Namespace string
	TaskQueue string
	PlanRepo  *store.PlanRepository
}

// CreateSubscription enforces per-user limit then starts the workflow with
// the Activate update in the same round trip, so the customer learns the
// outcome (active, trialing, or FailedPrecondition) before the RPC returns.
func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *subflowv1.CreateSubscriptionRequest) (*subflowv1.Subscription, error) {
	p, err := s.PlanRepo.Get(ctx, req.PlanId)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}

	if p.PerUserLimit > 0 {
		count, err := s.countActiveForUser(ctx, req.UserId, p.Code)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "count workflows: %v", err)
		}
		if count >= int64(p.PerUserLimit) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"per-user subscription limit reached for plan %q (%d/%d)",
				p.Code, count, p.PerUserLimit)
		}
	}

	now := time.Now().UTC()
	subID := uuid.NewString()
	periodEnd := now.Add(p.Cadence)
	if p.TrialDuration > 0 {
		periodEnd = now.Add(p.TrialDuration)
	}

	wfInput := workflow.SubscriptionInput{
		SubscriptionID: subID,
		IntervalID:     uuid.NewString(),
		UserID:         req.UserId,
		PlanID:         p.ID,
		Plan:           p,
		PeriodStart:    now,
		PeriodEnd:      periodEnd,
		Context:        workflow.Context(req.InitialContext),
	}

	initialPhase := "active"
	if p.TrialDuration > 0 {
		initialPhase = "trialing"
	}

	var trialEndsAt *time.Time
	if p.TrialDuration > 0 {
		trialEndsAt = &periodEnd
	}
	startVisibility := subflowtemporal.NewSubscriptionStartSearchAttributes(
		req.UserId, p.Code, initialPhase, periodEnd, trialEndsAt,
	)

	startOp := s.Temporal.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:                       "subscription:" + subID,
		TaskQueue:                s.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		TypedSearchAttributes:    startVisibility,
	}, workflow.SubscriptionWorkflow, wfInput)

	// For non-trial plans we wait for Completed (activation must succeed
	// before we return). For trial plans we only wait for Accepted —
	// the workflow is now in trialing phase and the Activate update will
	// be processed at trial end.
	waitFor := client.WorkflowUpdateStageCompleted
	if p.TrialDuration > 0 {
		waitFor = client.WorkflowUpdateStageAccepted
	}

	handle, err := s.Temporal.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName:   workflow.UpdateActivate,
			WaitForStage: waitFor,
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start workflow: %v", err)
	}

	resp := &subflowv1.Subscription{
		Id:          subID,
		UserId:      req.UserId,
		PlanCode:    p.Code,
		Phase:       initialPhase,
		PeriodStart: timestamppb.New(now),
		PeriodEnd:   timestamppb.New(periodEnd),
		Context:     map[string]string(wfInput.Context),
	}
	if p.TrialDuration == 0 {
		var result workflow.ActivationResult
		if err := handle.Get(ctx, &result); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "activation failed: %v", err)
		}
		resp.Phase = result.Phase
		resp.Context = map[string]string(result.Context)
	}
	return resp, nil
}

func (s *SubscriptionService) CancelSubscription(ctx context.Context, req *subflowv1.CancelSubscriptionRequest) (*subflowv1.CancelSubscriptionResponse, error) {
	if err := s.Temporal.SignalWorkflow(ctx, "subscription:"+req.Id, "", workflow.SignalCancelSubscription, struct{}{}); err != nil {
		return nil, status.Errorf(codes.NotFound, "signal workflow: %v", err)
	}
	return &subflowv1.CancelSubscriptionResponse{}, nil
}

func (s *SubscriptionService) UpdateSubscriptionContext(ctx context.Context, req *subflowv1.UpdateSubscriptionContextRequest) (*subflowv1.Subscription, error) {
	if err := s.Temporal.SignalWorkflow(ctx, "subscription:"+req.Id, "", workflow.SignalUpdateContext, req.Updates); err != nil {
		return nil, status.Errorf(codes.NotFound, "signal workflow: %v", err)
	}
	return s.GetSubscription(ctx, &subflowv1.GetSubscriptionRequest{Id: req.Id})
}

func (s *SubscriptionService) GetSubscription(ctx context.Context, req *subflowv1.GetSubscriptionRequest) (*subflowv1.Subscription, error) {
	res, err := s.Temporal.QueryWorkflow(ctx, "subscription:"+req.Id, "", workflow.QuerySubscriptionView)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "subscription not found: %v", err)
	}
	var v workflow.View
	if err := res.Get(&v); err != nil {
		return nil, status.Errorf(codes.Internal, "decode view: %v", err)
	}
	return viewToProto(req.Id, v), nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, req *subflowv1.ListSubscriptionsRequest) (*subflowv1.ListSubscriptionsResponse, error) {
	query := buildListQuery(req)
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 200 {
		pageSize = 200
	}
	resp, err := s.Temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     s.Namespace,
		Query:         query,
		PageSize:      pageSize,
		NextPageToken: []byte(req.PageToken),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflows: %v", err)
	}

	out := &subflowv1.ListSubscriptionsResponse{
		Subscriptions: make([]*subflowv1.Subscription, 0, len(resp.Executions)),
		NextPageToken: string(resp.NextPageToken),
	}
	for _, exec := range resp.Executions {
		subID := strings.TrimPrefix(exec.Execution.WorkflowId, "subscription:")
		full, err := s.GetSubscription(ctx, &subflowv1.GetSubscriptionRequest{Id: subID})
		if err != nil {
			continue // workflow may have just closed; skip
		}
		out.Subscriptions = append(out.Subscriptions, full)
	}
	return out, nil
}

func (s *SubscriptionService) countActiveForUser(ctx context.Context, userID, planCode string) (int64, error) {
	q := fmt.Sprintf(
		"WorkflowType='SubscriptionWorkflow' AND ExecutionStatus='Running' AND %s='%s' AND %s='%s' AND %s IN ('trialing','active','past_due','canceled')",
		subflowtemporal.AttrUserId, userID,
		subflowtemporal.AttrPlanCode, planCode,
		subflowtemporal.AttrPhase,
	)
	resp, err := s.Temporal.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: s.Namespace,
		Query:     q,
	})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func buildListQuery(req *subflowv1.ListSubscriptionsRequest) string {
	// ExecutionStatus='Running' excludes closed CAN runs — Visibility indexes
	// every run separately, but only the live run represents the subscription.
	parts := []string{"WorkflowType='SubscriptionWorkflow'", "ExecutionStatus='Running'"}
	if req.UserId != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'", subflowtemporal.AttrUserId, req.UserId))
	}
	if req.PlanCode != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'", subflowtemporal.AttrPlanCode, req.PlanCode))
	}
	if req.Phase != "" {
		parts = append(parts, fmt.Sprintf("%s='%s'", subflowtemporal.AttrPhase, req.Phase))
	}
	return strings.Join(parts, " AND ")
}

func viewToProto(id string, v workflow.View) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:                    id,
		UserId:                v.UserID,
		PlanCode:              v.PlanCode,
		Phase:                 v.Phase,
		PeriodStart:           timestamppb.New(v.PeriodStart),
		PeriodEnd:             timestamppb.New(v.PeriodEnd),
		RenewalCount:          int32(v.RenewalCount),
		Context:               map[string]string(v.Context),
		CancelRequested:       v.CancelRequested,
		DunningAttempt:        int32(v.DunningAttempt),
		LastChargedAt:         timestamppb.New(v.LastChargedAt),
		LastChargeAmountCents: v.LastChargeAmountCents,
		LastFailureAt:         timestamppb.New(v.LastFailureAt),
		LastFailureReason:     v.LastFailureReason,
		TotalChargedCents:     v.TotalChargedCents,
		SuccessfulChargeCount: int32(v.SuccessfulChargeCount),
		FailedChargeCount:     int32(v.FailedChargeCount),
	}
}
