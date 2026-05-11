package billing

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStore struct {
	col *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{col: db.Collection("billing_events")}
}

func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "reference", Value: 1}}, Options: options.Index().SetUnique(true).SetName("reference_unique")},
		{Keys: bson.D{{Key: "subscription_id", Value: 1}, {Key: "occurred_at", Value: -1}}, Options: options.Index().SetName("sub_occurred")},
		{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "occurred_at", Value: -1}}, Options: options.Index().SetName("user_occurred")},
	})
	return err
}

type eventDoc struct {
	ID             string    `bson:"_id"`
	SubscriptionID string    `bson:"subscription_id"`
	UserID         string    `bson:"user_id"`
	PlanCode       string    `bson:"plan_code"`
	Type           string    `bson:"type"`
	AmountCents    int64     `bson:"amount_cents"`
	Currency       string    `bson:"currency"`
	PeriodStart    time.Time `bson:"period_start"`
	PeriodEnd      time.Time `bson:"period_end"`
	RenewalCount   int       `bson:"renewal_count"`
	DunningAttempt int       `bson:"dunning_attempt"`
	TransactionID  string    `bson:"transaction_id"`
	FailureReason  string    `bson:"failure_reason"`
	OccurredAt     time.Time `bson:"occurred_at"`
	Reference      string    `bson:"reference"`
}

func (s *MongoStore) Append(ctx context.Context, ev Event) error {
	doc := eventDoc{
		ID:             ev.ID,
		SubscriptionID: ev.SubscriptionID,
		UserID:         ev.UserID,
		PlanCode:       ev.PlanCode,
		Type:           ev.Type,
		AmountCents:    ev.AmountCents,
		Currency:       ev.Currency,
		PeriodStart:    ev.PeriodStart,
		PeriodEnd:      ev.PeriodEnd,
		RenewalCount:   ev.RenewalCount,
		DunningAttempt: ev.DunningAttempt,
		TransactionID:  ev.TransactionID,
		FailureReason:  ev.FailureReason,
		OccurredAt:     ev.OccurredAt,
		Reference:      ev.Reference,
	}
	_, err := s.col.InsertOne(ctx, doc)
	if err != nil && mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (s *MongoStore) List(ctx context.Context, q ListQuery) ([]Event, string, error) {
	size := q.PageSize
	if size <= 0 {
		size = DefaultPageSize
	}
	if size > MaxPageSize {
		size = MaxPageSize
	}

	filter := bson.M{}
	if q.SubscriptionID != "" {
		filter["subscription_id"] = q.SubscriptionID
	}
	if q.UserID != "" {
		filter["user_id"] = q.UserID
	}
	if q.TypeFilter != "" {
		filter["type"] = q.TypeFilter
	}
	if q.PageCursor != "" {
		cursorTime, err := decodeCursor(q.PageCursor)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page_cursor: %w", err)
		}
		filter["occurred_at"] = bson.M{"$lt": cursorTime}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "occurred_at", Value: -1}}).
		SetLimit(int64(size + 1))

	cur, err := s.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", err
	}
	defer cur.Close(ctx)

	out := make([]Event, 0, size)
	for cur.Next(ctx) {
		var d eventDoc
		if err := cur.Decode(&d); err != nil {
			return nil, "", err
		}
		out = append(out, docToEvent(d))
	}
	if err := cur.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(out) > size {
		nextCursor = encodeCursor(out[size-1].OccurredAt)
		out = out[:size]
	}
	return out, nextCursor, nil
}

func docToEvent(d eventDoc) Event {
	return Event{
		ID:             d.ID,
		SubscriptionID: d.SubscriptionID,
		UserID:         d.UserID,
		PlanCode:       d.PlanCode,
		Type:           d.Type,
		AmountCents:    d.AmountCents,
		Currency:       d.Currency,
		PeriodStart:    d.PeriodStart,
		PeriodEnd:      d.PeriodEnd,
		RenewalCount:   d.RenewalCount,
		DunningAttempt: d.DunningAttempt,
		TransactionID:  d.TransactionID,
		FailureReason:  d.FailureReason,
		OccurredAt:     d.OccurredAt,
		Reference:      d.Reference,
	}
}

func encodeCursor(t time.Time) string {
	return base64.URLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano)))
}

func decodeCursor(c string) (time.Time, error) {
	raw, err := base64.URLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, string(raw))
	if err != nil {
		return time.Time{}, err
	}
	if t.IsZero() {
		return time.Time{}, errors.New("zero cursor time")
	}
	return t, nil
}
