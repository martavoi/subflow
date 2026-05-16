# Workflow-native Subscription aggregate

The Subscription aggregate (struct, `Phase` enum, `BillingPeriod` value object, `SubscriptionInput`, `Context`, and all behavior methods) lives in `internal/workflow`, not in a separate `internal/domain/subscription` package. Plan retains its own `internal/domain/plan` package because Plan has independent Mongo persistence and lifecycle.

## Why

The Temporal workflow execution IS the Subscription aggregate root: state lives in workflow memory, persistence is the workflow's history, and behavior is methods on the entity that orchestrate activities. A separate domain package would either (a) duplicate the struct and force translation across the boundary, or (b) accept that its methods take `workflow.Context` — which makes the layer's "domain purity" a fiction. Both alternatives add ceremony without paying back; collapsing is the honest shape.

## Considered

Strict DDD layering (Subscription in `domain/`, workflow as a thin orchestration shell) was rejected because every aggregate method would carry `workflow.Context`. The domain layer would be hard-coupled to the Temporal SDK, defeating the separation it claims.

## Consequences

A reader familiar with classic Go-DDD layouts will look for `internal/domain/subscription` and not find one. The Subscription file in `internal/workflow` should open with a short comment naming this choice so readers don't assume the omission is accidental.
