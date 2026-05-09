package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
)

// Event is the canonical lifecycle event payload published to downstream
// consumers. Stable shape so swapping the stub for Kafka later is a one-file
// change.
type Event struct {
	Reference      string               `json:"reference"`
	Type           string               `json:"type"`
	SubscriptionID string               `json:"subscription_id"`
	UserID         string               `json:"user_id"`
	PlanID         string               `json:"plan_id"`
	PlanCode       string               `json:"plan_code"`
	PeriodStart    time.Time            `json:"period_start"`
	PeriodEnd      time.Time            `json:"period_end"`
	RenewalCount   int                  `json:"renewal_count"`
	Context        subscription.Context `json:"context"`
	OccurredAt     time.Time            `json:"occurred_at"`
}

// Publisher is the swap point. Stub writes to stdout; a real Kafka publisher
// would implement this interface.
type Publisher interface {
	Publish(ctx context.Context, ev Event) error
}

// StdoutPublisher writes events to stdout as one JSON object per line.
type StdoutPublisher struct {
	Logger *slog.Logger
}

func NewStdoutPublisher(logger *slog.Logger) *StdoutPublisher {
	return &StdoutPublisher{Logger: logger}
}

func (p *StdoutPublisher) Publish(_ context.Context, ev Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "subflow.event "+string(b)); err != nil {
		return err
	}
	if p.Logger != nil {
		p.Logger.Info("event published", slog.String("type", ev.Type), slog.String("ref", ev.Reference))
	}
	return nil
}
