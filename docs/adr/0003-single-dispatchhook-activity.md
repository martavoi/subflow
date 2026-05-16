# Single generic DispatchHook activity

All Hook events flow through one Temporal activity, `DispatchHook`, that takes the hook `Type` and event payload as input. The `SubscriptionHooks` gRPC service exposes one rpc, `Dispatch(Event)`, with a `oneof` payload variant for lifecycle vs payment data. There are not 11 activities, one per Hook type; there is one.

## Why

Eleven near-identical activities (`OnTrialStarted`, `OnRenewed`, ...) were rejected as ceremony — each was a one-line wrapper around the same internal dispatcher. Collapsing to one removes the worker-registration boilerplate, simplifies the proto schema from 11 rpcs to 1, and reduces the Hook value object to a single string-typed enum (`hook.Type`) with no `ActivityName` field and no `HookByName` lookup. The workflow reads `s.fireHook(ctx, hook.Activated)` — direct.

## Consequences

The Temporal Web UI now shows `DispatchHook` for every Hook execution row, rather than the Hook name. To identify which Hook fired, the reader expands the activity input. Mitigation: the `Type` is the first field of the input payload, immediately visible on expand. Live demos lose one piece of at-a-glance readability in exchange for code-shape simplicity — accepted trade-off for a learning project where the workflow code itself is the primary artifact being demonstrated.
