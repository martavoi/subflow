package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/hook"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrPlanNotFound = errors.New("plan not found")

type planDoc struct {
	ID                   string          `bson:"_id"`
	Code                 string          `bson:"code"`
	Name                 string          `bson:"name"`
	Cadence              string          `bson:"cadence"`
	PriceCents           int64           `bson:"price_cents"`
	Currency             string          `bson:"currency"`
	PerUserLimit         int             `bson:"per_user_limit"`
	TrialDuration        string          `bson:"trial_duration"`
	TrialEndNoticeBefore string          `bson:"trial_end_notice_before"`
	DunningMaxAttempts   int             `bson:"dunning_max_attempts"`
	DunningRetryBackoff  string          `bson:"dunning_retry_backoff"`
	IntegrationEndpoint  string          `bson:"integration_endpoint"`
	EnabledHooks         []hook.Type `bson:"enabled_hooks"`
	CreatedAt            time.Time       `bson:"created_at"`
}

type PlanRepository struct {
	col *mongo.Collection
}

func NewPlanRepository(db *mongo.Database) *PlanRepository {
	return &PlanRepository{col: db.Collection("plans")}
}

// EnsureIndexes creates required indexes. Idempotent.
func (r *PlanRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "code", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("plans_code_unique"),
	})
	return err
}

func (r *PlanRepository) Insert(ctx context.Context, p plan.Plan) error {
	_, err := r.col.InsertOne(ctx, planToDoc(p))
	return err
}

func (r *PlanRepository) Get(ctx context.Context, id string) (plan.Plan, error) {
	var d planDoc
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return plan.Plan{}, ErrPlanNotFound
	}
	if err != nil {
		return plan.Plan{}, err
	}
	return docToPlan(d)
}

func (r *PlanRepository) GetByCode(ctx context.Context, code string) (plan.Plan, error) {
	var d planDoc
	err := r.col.FindOne(ctx, bson.M{"code": code}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return plan.Plan{}, ErrPlanNotFound
	}
	if err != nil {
		return plan.Plan{}, err
	}
	return docToPlan(d)
}

func (r *PlanRepository) List(ctx context.Context) ([]plan.Plan, error) {
	cur, err := r.col.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]plan.Plan, 0)
	for cur.Next(ctx) {
		var d planDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		p, err := docToPlan(d)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, cur.Err()
}

func (r *PlanRepository) Delete(ctx context.Context, id string) error {
	res, err := r.col.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return ErrPlanNotFound
	}
	return nil
}

func planToDoc(p plan.Plan) planDoc {
	return planDoc{
		ID:                   p.ID,
		Code:                 p.Code,
		Name:                 p.Name,
		Cadence:              p.Cadence.String(),
		PriceCents:           p.PriceCents,
		Currency:             p.Currency,
		PerUserLimit:         p.PerUserLimit,
		TrialDuration:        durationOrEmpty(p.TrialDuration),
		TrialEndNoticeBefore: durationOrEmpty(p.TrialEndNoticeBefore),
		DunningMaxAttempts:   p.DunningMaxAttempts,
		DunningRetryBackoff:  durationOrEmpty(p.DunningRetryBackoff),
		IntegrationEndpoint:  p.IntegrationEndpoint,
		EnabledHooks:         append([]hook.Type(nil), p.EnabledHooks...),
		CreatedAt:            p.CreatedAt,
	}
}

func docToPlan(d planDoc) (plan.Plan, error) {
	cadence, err := time.ParseDuration(d.Cadence)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse cadence %q: %w", d.Cadence, err)
	}
	trial, err := parseOptional(d.TrialDuration)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse trial_duration: %w", err)
	}
	notice, err := parseOptional(d.TrialEndNoticeBefore)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse trial_end_notice_before: %w", err)
	}
	backoff, err := parseOptional(d.DunningRetryBackoff)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse dunning_retry_backoff: %w", err)
	}
	return plan.Plan{
		ID:                   d.ID,
		Code:                 d.Code,
		Name:                 d.Name,
		Cadence:              cadence,
		PriceCents:           d.PriceCents,
		Currency:             d.Currency,
		PerUserLimit:         d.PerUserLimit,
		TrialDuration:        trial,
		TrialEndNoticeBefore: notice,
		DunningMaxAttempts:   d.DunningMaxAttempts,
		DunningRetryBackoff:  backoff,
		IntegrationEndpoint:  d.IntegrationEndpoint,
		EnabledHooks:         append([]hook.Type(nil), d.EnabledHooks...),
		CreatedAt:            d.CreatedAt,
	}, nil
}

// durationOrEmpty formats an optional duration as either "" or its Go string.
// parseOptional inverts it.
func durationOrEmpty(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}

func parseOptional(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
