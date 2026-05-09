package store

import (
	"context"
	"errors"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrSubscriptionNotFound = errors.New("subscription not found")

// SubscriptionView is the read-model projection record.
type SubscriptionView struct {
	ID              string               `bson:"_id"`
	UserID          string               `bson:"user_id"`
	PlanID          string               `bson:"plan_id"`
	Phase           string               `bson:"phase"`
	PeriodStart     time.Time            `bson:"period_start"`
	PeriodEnd       time.Time            `bson:"period_end"`
	RenewalCount    int                  `bson:"renewal_count"`
	Context         subscription.Context `bson:"context"`
	CancelRequested bool                 `bson:"cancel_requested"`
	UpdatedAt       time.Time            `bson:"updated_at"`
}

type SubscriptionProjectionRepository struct {
	col *mongo.Collection
}

func NewSubscriptionProjectionRepository(db *mongo.Database) *SubscriptionProjectionRepository {
	return &SubscriptionProjectionRepository{col: db.Collection("subscriptions_view")}
}

func (r *SubscriptionProjectionRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "user_id", Value: 1}}, Options: options.Index().SetName("user_id_idx")},
		{Keys: bson.D{{Key: "phase", Value: 1}}, Options: options.Index().SetName("phase_idx")},
	})
	return err
}

func (r *SubscriptionProjectionRepository) Upsert(ctx context.Context, v SubscriptionView) error {
	v.UpdatedAt = time.Now().UTC()
	_, err := r.col.ReplaceOne(ctx,
		bson.M{"_id": v.ID}, v,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (r *SubscriptionProjectionRepository) Get(ctx context.Context, id string) (SubscriptionView, error) {
	var v SubscriptionView
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&v)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return SubscriptionView{}, ErrSubscriptionNotFound
	}
	return v, err
}

func (r *SubscriptionProjectionRepository) List(ctx context.Context, userID, phase string) ([]SubscriptionView, error) {
	filter := bson.M{}
	if userID != "" {
		filter["user_id"] = userID
	}
	if phase != "" {
		filter["phase"] = phase
	}
	cur, err := r.col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]SubscriptionView, 0)
	for cur.Next(ctx) {
		var v SubscriptionView
		if err := cur.Decode(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, cur.Err()
}
