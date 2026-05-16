# Hooks are pure-notification — no synchronous integrator approval

Integrators receive Hook events asynchronously and fire-and-forget; the workflow does not branch on the integrator's response. Integrators that need to influence Subscription state call back via the `UpdateSubscriptionContext` signal, which mutates the Context that subsequent Hooks carry.

## Why

A synchronous pre-payment hook (e.g. fraud check, eligibility veto) was considered and rejected on two grounds:

1. **It's not how classic subscription platforms work.** Stripe, Recurly, Chargebee, and Paddle expose only async notification webhooks. Fraud lives inside the payment processor (Stripe Radar), not in an integrator-supplied veto. The one open-source platform that does support pre-payment control — KillBill — does so through in-process JVM plugins, not over the wire. A remote gRPC veto has no precedent in the space.

2. **It would invert the SLA dependency the wrong way.** A synchronous integrator call on the renewal path means: integrator down ⇒ renewals stuck on retries; integrator latency ⇒ added latency on every Charge. The inverted-control pattern (integrator reacts and calls back via `UpdateSubscriptionContext`) keeps renewals self-sufficient.

The only place synchronous semantics are needed is Activation: the API caller wants to know whether the first Charge succeeded. That is handled by `UpdateWithStartWorkflow` returning the Activation outcome to the caller, not by a Hook. The caller already has a synchronous answer through Temporal's own primitives; no Hook is needed and none is provided.

## Consequences

If a future requirement genuinely needs synchronous integrator influence (e.g. SCA / 3DS handoff), it should be added as a new Temporal primitive (Update with a typed response), not as a "blocking Hook". Hooks remain notification-only, forever.
