package billing

import "context"

// EventStore is the interface every billing event backend implements.
// MongoStore is the current concrete; a future KafkaStore can swap in
// without changing the workflow or activity layer.
type EventStore interface {
	// Append writes an Event. Implementations must be idempotent on
	// Event.Reference (a retry with the same reference returns nil
	// without inserting a duplicate).
	Append(ctx context.Context, ev Event) error

	// List returns events matching the query, newest first, and a cursor
	// for the next page (empty when no more pages).
	List(ctx context.Context, q ListQuery) (events []Event, nextCursor string, err error)
}
