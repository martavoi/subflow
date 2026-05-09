package plan

import "time"

// Plan is a subscription plan aggregate. Persisted in the plans collection.
type Plan struct {
	ID                  string
	Code                string
	Name                string
	BillingInterval     time.Duration // parsed from Go duration syntax
	PriceCents          int64
	IntegrationEndpoint string
	CreatedAt           time.Time
}
