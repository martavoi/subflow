package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/billing"
	"go.temporal.io/sdk/workflow"
)

// chargePurpose is a closed enum for the charge call sites. Avoids the
// stringly-typed purpose argument the original design carried around.
type chargePurpose string

const (
	chargeActivation chargePurpose = "activation"
	chargeRenewal    chargePurpose = "renewal"
	chargeDunning    chargePurpose = "dunning"
)

// Charge attempts a single payment, records the resulting billing event,
// updates entity summary fields, and fires the matching payment hook.
// Returns the original charge error so callers (Activate, Renew,
// HandleDunning) can route the workflow accordingly.
func (s *Subscription) Charge(ctx workflow.Context, purpose chargePurpose, dunningAttempt int) error {
	ref := s.idempotencyKey(ctx, fmt.Sprintf("charge:%s:%d", purpose, dunningAttempt))

	chargeOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.ChargePaymentRetry,
	})
	chargeIn := activity.ChargePaymentInput{
		Reference:   ref,
		UserID:      s.UserID,
		PlanCode:    s.PlanCode,
		AmountCents: s.Plan.PriceCents,
		Currency:    s.Plan.Currency,
	}
	var chargeRes activity.ChargePaymentResult
	chargeErr := workflow.ExecuteActivity(chargeOpts, "ChargePayment", chargeIn).Get(ctx, &chargeRes)

	s.recordBillingEvent(ctx, ref, dunningAttempt, chargeRes, chargeErr)
	s.applyChargeOutcome(ctx, chargeRes, chargeErr, dunningAttempt)

	return chargeErr
}

// recordBillingEvent writes an append-only billing record for this charge
// attempt. Idempotent at the EventStore layer. Log-and-continue on permanent
// failure — the charge already happened; workflow history is the forensic
// fallback if Mongo is permanently down.
func (s *Subscription) recordBillingEvent(ctx workflow.Context, ref string, dunningAttempt int, res activity.ChargePaymentResult, chargeErr error) {
	now := workflow.Now(ctx)
	eventType := billing.TypeCharged
	failureReason := ""
	if chargeErr != nil {
		eventType = billing.TypeFailed
		failureReason = chargeErr.Error()
	}
	ev := billing.Event{
		ID:             ref,
		SubscriptionID: s.ID,
		UserID:         s.UserID,
		PlanCode:       s.PlanCode,
		Type:           eventType,
		AmountCents:    s.Plan.PriceCents,
		Currency:       s.Plan.Currency,
		PeriodStart:    s.Period.Start,
		PeriodEnd:      s.Period.End,
		RenewalCount:   s.RenewalCount,
		DunningAttempt: dunningAttempt,
		TransactionID:  res.TransactionID,
		FailureReason:  failureReason,
		OccurredAt:     now,
		Reference:      ref,
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         activity.BillingEventRetry,
	})
	_ = workflow.ExecuteActivity(opts, "RecordBillingEvent", ev).Get(ctx, nil)
}

// applyChargeOutcome updates the entity's O(1) summary fields and fires the
// payment hook.
func (s *Subscription) applyChargeOutcome(ctx workflow.Context, res activity.ChargePaymentResult, chargeErr error, dunningAttempt int) {
	now := workflow.Now(ctx)
	if chargeErr == nil {
		s.LastChargedAt = now
		s.LastChargeAmountCents = s.Plan.PriceCents
		s.TotalChargedCents += s.Plan.PriceCents
		s.SuccessfulChargeCount++
		_ = s.FirePaymentHook(ctx, HookPaymentOK, dunningAttempt, res.TransactionID, "")
		return
	}
	s.LastFailureAt = now
	s.LastFailureReason = chargeErr.Error()
	s.FailedChargeCount++
	_ = s.FirePaymentHook(ctx, HookPaymentFailed, dunningAttempt, "", chargeErr.Error())
}
