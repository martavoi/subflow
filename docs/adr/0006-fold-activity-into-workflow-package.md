# Fold internal/activity into internal/workflow

The package `internal/activity` is removed. Its files move into `internal/workflow` with an `activity_` filename prefix, matching the existing `phase_` convention. The Temporal worker registers handlers from a single `workflow` package.

## Why

Activities and workflows live in the same Go package in canonical Temporal Go references (`temporalio/samples-go`, the official ecommerce and expense tutorials). Subflow's split added an import boundary without paying back: workflows still depended on the activity package by name, the activity package still depended on `internal/billing` and `internal/integration`, and the two packages always shipped together.

## Consequences

The `activity_` filename prefix is the only convention separating activity handlers from workflow code. Go has no language-level guard against calling an activity handler function directly from a workflow function (which would silently execute non-deterministically). Code review and the filename hint are the safeguards.
