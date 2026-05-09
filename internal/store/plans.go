package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrPlanNotFound = errors.New("plan not found")

type planDoc struct {
	ID                  string    `bson:"_id"`
	Code                string    `bson:"code"`
	Name                string    `bson:"name"`
	BillingInterval     string    `bson:"billing_interval"` // stored as Go duration string for human-readable docs
	PriceCents          int64     `bson:"price_cents"`
	IntegrationEndpoint string    `bson:"integration_endpoint"`
	CreatedAt           time.Time `bson:"created_at"`
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
	_, err := r.col.InsertOne(ctx, planDoc{
		ID:                  p.ID,
		Code:                p.Code,
		Name:                p.Name,
		BillingInterval:     p.BillingInterval.String(),
		PriceCents:          p.PriceCents,
		IntegrationEndpoint: p.IntegrationEndpoint,
		CreatedAt:           p.CreatedAt,
	})
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

func docToPlan(d planDoc) (plan.Plan, error) {
	dur, err := time.ParseDuration(d.BillingInterval)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse billing_interval %q: %w", d.BillingInterval, err)
	}
	return plan.Plan{
		ID:                  d.ID,
		Code:                d.Code,
		Name:                d.Name,
		BillingInterval:     dur,
		PriceCents:          d.PriceCents,
		IntegrationEndpoint: d.IntegrationEndpoint,
		CreatedAt:           d.CreatedAt,
	}, nil
}
