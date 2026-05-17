# Rename Status to View

The query previously named `subscription.status` returns a read-model snapshot of the subscription for API rendering. Rename type `workflow.Status` to `workflow.View`, query name to `subscription.view`, constant to `QuerySubscriptionView`, handler method to `(*Subscription).View()`.

## Why

`CONTEXT.md` lists "Status" under `_Avoid_` for `Phase`. Using "Status" both for a synonym of the lifecycle state and for the query return type conflated two concepts. The thing the query returns is read-model data (current phase, period boundaries, charge counters, context) — not a lifecycle state. "View" is the CQRS-shaped term for a read-model projection and aligns cleanly with how the API consumes it.

## Consequences

The `Phase` enum and its values are unchanged. The `subscription.view` glossary entry is added to `CONTEXT.md` so future readers know the read-model term.
