# Integration-event contract: hook.Event with sealed payload

The integration-event payload published to integrators is `hook.Event` — a canonical struct carrying common identity/context fields and a sealed-interface `hook.Payload`. Payload variants are `hook.LifecyclePayload` and `hook.PaymentPayload` today; new variants are additive.

## Why

The previous design had two near-identical workflow methods (`fireLifecycle`, `firePayment`) plus two nullable pointers (`*LifecycleData`, `*PaymentData`) on the activity input. Adding a new payload category would have meant another nullable pointer, another switch arm, and another near-duplicate emit function. The sealed interface makes the payload required and typed: workflow method count grows with payload categories (two today) rather than hook types (eleven today).

## Considered

- **Kafka as the integration substrate.** Out of scope for this stage. Reporting and pub-sub fan-out can come later via a Kafka producer activity without re-shaping `hook.Event`.
- **One method per hook type** (`emitTrialStarted`, `emitRenewed`, ...). Rejected: eleven methods that mostly duplicate boilerplate.
- **One emit method with `any` payload.** Rejected: payload could be any type, losing compile-time guarantees that integrator-facing payloads contain only known shapes.

## Consequences

Hook events are integration events, not domain events in the DDD sense. They are published outward; the workflow does not consume its own emissions to drive internal state. The Subscription aggregate (per ADR 0001) drives state changes directly through method calls, not through an internal event bus.
