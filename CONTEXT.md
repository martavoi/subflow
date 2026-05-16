# Subflow

Subflow models a subscription lifecycle on top of Temporal. Plans configure pricing, cadence, trials, dunning, and which integrator hooks fire. Subscriptions live as long-running entity workflows.

## Language

**Subscription**:
The aggregate that tracks one customer's ongoing relationship to a Plan. Has a Phase, a current Billing Period, and a Context.
_Avoid_: Subscription Order, Membership

**Plan**:
The pricing and cadence template a Subscription is bound to at creation. Carries price, cadence, trial config, dunning config, integration endpoint, and enabled Hooks.
_Avoid_: Product, Tier, Package

**Phase**:
The current lifecycle state of a Subscription. One of `trialing`, `active`, `past_due`, `canceled`, `deactivated`. (Plus the transient `starting` during creation.)
_Avoid_: State, Status

**Billing Period**:
A half-open interval `[start, end)` during which a Subscription is billed for one cycle. The first Billing Period of a trial Plan is the Trial; subsequent Billing Periods are paid Renewals.
_Avoid_: Cycle, Term, Subscription Period

**Trial**:
The first Billing Period of a Subscription whose Plan has a trial duration. Zero-charge. Cancel during a Trial is immediate, not end-of-period.
_Avoid_: Free Period, Grace Period

**Activation**:
The transition into the first paid Phase. Caused by the first successful Charge — either at Subscription creation (no-trial Plans) or at trial end (trial Plans).
_Avoid_: Start, Begin

**Renewal**:
A paid Charge at the end of a Billing Period that succeeds. Renewals have no synchronous caller; the workflow is the only initiator. A failed Renewal routes to Dunning.
_Avoid_: Rebilling, Renewal Cycle

**Dunning**:
The retry loop after a failed Renewal Charge. Exponential backoff, configurable maximum attempts per Plan. Success transitions to Recovered (active); exhaustion transitions to Deactivation.
_Avoid_: Retry, Recovery Loop

**Cancel**:
An integrator request to terminate a Subscription. **End-of-period semantics**: the Subscription stays `active` until the current Billing Period ends, then transitions to `deactivated`. A Cancel during Trial is the exception — it deactivates immediately.
_Avoid_: Terminate, End

**Deactivation**:
The terminal Phase. No further Charges. After the Deactivation Hook fires, the Subscription's workflow run completes.
_Avoid_: Suspended, Closed

**Hook**:
A pure-notification event the integrator subscribes to per-Plan. Fire-and-forget — the workflow does not branch on the response. Integrators that need to influence Subscription state mutate the Context via `UpdateSubscriptionContext`; subsequent Hooks carry the new Context.
_Avoid_: Webhook (implies HTTP), Callback (implies synchronous return)

**Context**:
A per-Subscription mutable key-value bag carried on every Hook event. Integrators write back via the `UpdateSubscriptionContext` signal.
_Avoid_: Metadata, Attributes, Properties

## Relationships

- A **Plan** has zero or more **Subscriptions**
- A **Subscription** belongs to exactly one **Plan** (snapshot frozen at creation)
- A **Subscription** has one current **Phase** and one current **Billing Period**
- The first **Billing Period** of a trial **Plan**'s **Subscription** is its **Trial**
- A failed **Renewal** transitions the **Subscription** to `past_due` and starts **Dunning**
- A **Cancel** during `active`/`past_due` defers **Deactivation** to the end of the current **Billing Period**; a **Cancel** during `trialing` deactivates immediately
- A **Plan** declares which **Hooks** its **Subscriptions** emit
- Every **Hook** carries the current **Context**

## Example dialogue

> **Dev:** "If a customer cancels three days into a 30-day Billing Period, when do they lose access?"
> **Domain expert:** "End of the period. The Subscription stays `active` for 27 more days, then deactivates. Trial is the exception — cancel mid-trial means deactivate now."

> **Dev:** "What happens if the renewal Charge fails?"
> **Domain expert:** "Dunning starts. We retry on the Plan's backoff schedule up to the configured max. Success means Recovered, back to `active`. Exhaustion means Deactivation."

> **Dev:** "Can the integrator veto a Charge?"
> **Domain expert:** "No. Hooks are pure-notification — we tell integrators what happened, we don't ask permission. If they need to influence behavior, they update the Subscription's Context, which subsequent Hooks carry."

## Flagged ambiguities

- **"Hook" vs "Webhook" vs "Notification"** — resolved: **Hook** is the term. "Webhook" implies HTTP (Subflow uses gRPC); "Notification" is the *semantics* (vs synchronous), not the concept name.
- **"Activation"** was overloaded between "Subscription is created" and "first paid Charge succeeds" — resolved: **Activation** = first paid Charge succeeds. Subscription creation has no domain term; it's just "creating a Subscription".
- **"Cancel" vs "Deactivate"** — resolved: **Cancel** is the integrator request; **Deactivation** is the resulting terminal state. They are separated in time for paid Subscriptions (end-of-period); collapsed in time for Trials.
