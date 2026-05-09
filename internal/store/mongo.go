package store

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Connect dials Mongo and returns a *mongo.Database for the given database name.
func Connect(ctx context.Context, uri, database string) (*mongo.Client, *mongo.Database, error) {
	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, nil, fmt.Errorf("mongo ping: %w", err)
	}
	return client, client.Database(database), nil
}
