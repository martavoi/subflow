# Drop plan.Snapshot

The workflow input carries `plan.Plan` directly. The `plan.Snapshot` type and `plan.SnapshotOf` constructor are removed.

## Why

`Snapshot` existed to expose only the workflow-relevant subset of `Plan` and to hide persistence-only fields (`ID`, `Name`, `PerUserLimit`, `CreatedAt`) from workflow code. In practice the cost (parallel struct, parallel constructor, parallel test fixtures) exceeded the value (saving four fields of memory and a comment). The workflow only reads the fields it needs from `plan.Plan`; the extra fields ride along unused.

## Consequences

Adding a new plan field is one place to update instead of two. The external API payload that starts a subscription workflow is unchanged; Plan's extra fields ride along inside workflow history only.
