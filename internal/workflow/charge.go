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
//
// Billing-event write is log-and-continue on permanent failure — the charge
// already happened; workflow history is the forensic fallback if Mongo is
// permanently down.
func (s *Subscription) Charge(ctx workflow.Context, purpose chargePurpose, dunningAttempt int) error {
	ref := s.idempotencyKey(fmt.Sprintf("charge:%s:%d", purpose, dunningAttempt))

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

	now := workflow.Now(ctx)
	eventType := billing.TypeCharged
	failureReason := ""
	if chargeErr != nil {
		eventType = billing.TypeFailed
		failureReason = chargeErr.Error()
	}
	ev := billing.Event{
		ID:             ref,
		SubscriptionID: s.SubscriptionID,
		UserID:         s.UserID,
		PlanCode:       s.PlanCode,
		Type:           eventType,
		AmountCents:    s.Plan.PriceCents,
		Currency:       s.Plan.Currency,
		PeriodStart:    s.Period.Start,
		PeriodEnd:      s.Period.End,
		RenewalCount:   s.RenewalCount,
		DunningAttempt: dunningAttempt,
		TransactionID:  chargeRes.TransactionID,
		FailureReason:  failureReason,
		OccurredAt:     now,
		Reference:      ref,
	}
	billingOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         activity.BillingEventRetry,
	})
	_ = workflow.ExecuteActivity(billingOpts, "RecordBillingEvent", ev).Get(ctx, nil)

	if chargeErr == nil {
		s.LastChargedAt = now
		s.LastChargeAmountCents = s.Plan.PriceCents
		s.TotalChargedCents += s.Plan.PriceCents
		s.SuccessfulChargeCount++
		_ = s.FirePaymentHook(ctx, HookPaymentOK, dunningAttempt, chargeRes.TransactionID, "")
	} else {
		s.LastFailureAt = now
		s.LastFailureReason = chargeErr.Error()
		s.FailedChargeCount++
		_ = s.FirePaymentHook(ctx, HookPaymentFailed, dunningAttempt, "", chargeErr.Error())
	}

	return chargeErr
}
