# subflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Temporal-backed subscription lifecycle POC in Go: gRPC API + worker + mock integration, demoing durable workflows replacing a polling scheduler, with Continue-As-New per renewal, Mongo for plans/projection, and a podman/docker compose stack.

**Architecture:** Three Go binaries (`subflow-api`, `subflow-worker`, `mock-integration`). DDD-shaped workflow code: `SubscriptionWorkflow` orchestrates exported lifecycle verbs (`ActivateSubscription`, `RenewSubscription`, `DeactivateSubscription`, `AwaitPeriodEndOrCancellation`, `ContinueIntoNextPeriod`). Activities for payment, event publish, integration callout, projection update, each with a named `RetryPolicy`. Mongo holds plans + a read-model projection. Temporal's dev server (SQLite) stores workflow state and serves the bundled Web UI on `:8233`.

**Tech Stack:** Go 1.23, `go.temporal.io/sdk`, `go.mongodb.org/mongo-driver/v2`, `google.golang.org/grpc`, `buf` for proto codegen, `log/slog`, podman/docker compose.

---

## File Structure (locks in decomposition)

```
subflow/
├── go.mod, go.sum
├── .gitignore, .env.example, README.md, LICENSE
├── Taskfile.yml                       # task runner
├── compose.yml                        # 5 services
├── buf.yaml, buf.gen.yaml
├── api/v1/
│   ├── subflow.proto                  # SubflowService
│   ├── integration.proto              # IntegrationService (mock external)
│   └── *.pb.go (generated)
├── cmd/
│   ├── api/main.go + Dockerfile       # gRPC server
│   ├── worker/main.go + Dockerfile    # Temporal worker
│   └── mock-integration/main.go + Dockerfile
├── internal/
│   ├── config/config.go               # env-var loader
│   ├── domain/
│   │   ├── subscription/
│   │   │   ├── input.go               # SubscriptionInput value object
│   │   │   ├── period.go              # NextBillingPeriod (pure)
│   │   │   ├── period_test.go
│   │   │   └── context.go             # SubscriptionContext type alias + helpers
│   │   └── plan/plan.go               # Plan aggregate
│   ├── workflow/
│   │   ├── subscription.go            # SubscriptionWorkflow entry
│   │   ├── lifecycle.go               # exported lifecycle verbs
│   │   ├── signals.go                 # signal/query name constants
│   │   ├── state.go                   # in-run state + query handler
│   │   └── subscription_test.go       # testsuite tests
│   ├── activity/
│   │   ├── errors.go                  # error type names (terminal vs transient)
│   │   ├── retry.go                   # named RetryPolicy values
│   │   ├── payment.go                 # ChargePayment
│   │   ├── events.go                  # PublishSubscriptionEvent
│   │   ├── integration.go             # NotifyIntegrationService
│   │   └── projection.go              # UpdateSubscriptionProjection
│   ├── server/
│   │   ├── plans.go                   # Plan RPC handlers
│   │   └── subscriptions.go           # Subscription RPC handlers
│   ├── store/
│   │   ├── mongo.go                   # Mongo connection
│   │   ├── plans.go                   # PlanRepository
│   │   └── projection.go              # SubscriptionProjectionRepository
│   ├── eventbus/stub.go               # Publisher interface + stdout impl
│   └── integration/client.go          # gRPC client to mock-integration
└── docs/
    ├── architecture.md
    ├── temporal-cheatsheet.md
    └── superpowers/{specs,plans}/
```

**File responsibility rules:**
- `domain/*` files contain only deterministic, framework-agnostic types and pure functions.
- `workflow/*` files contain only Temporal-replay-safe code (no `time.Now`, no I/O, no `math/rand`).
- `activity/*` files contain Go functions with arbitrary I/O — they're called by Temporal but otherwise unconstrained.
- `server/*` files translate gRPC to Temporal/Mongo calls, no business logic.
- `store/*` files own Mongo I/O.

---

## Task 1: Repo bootstrap

**Files:**
- Create: `subflow/go.mod`
- Create: `subflow/.gitignore`
- Create: `subflow/.env.example`
- Create: `subflow/LICENSE`

**Note:** The `subflow/` directory and a `docs/` subdirectory already exist (from spec writing). All paths below are relative to `subflow/` unless stated otherwise.

- [ ] **Step 1: Initialize Go module**

Run from `~/Projects/subflow`:
```bash
go mod init github.com/martavoi/subflow
```
Expected: creates `go.mod` with `module github.com/martavoi/subflow` and a `go 1.23` line.

- [ ] **Step 2: Create `.gitignore`**

Write `.gitignore`:
```
# Binaries
/bin/
/cmd/api/api
/cmd/worker/worker
/cmd/mock-integration/mock-integration

# Local env
.env
*.local

# IDE
.idea/
.vscode/

# OS
.DS_Store

# Volumes (for podman/docker compose-managed bind mounts only)
/.data/

# Generated
*.pb.go.bak
```

- [ ] **Step 3: Create `.env.example`**

Write `.env.example`:
```
# subflow-api
API_GRPC_PORT=50051
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=subflow

# subflow-worker
TASK_QUEUE=subflow
INTEGRATION_HOST=localhost:50052

# mock-integration
MOCK_GRPC_PORT=50052
FAILURE_RATE=0.3
LATENCY_MS=100
TERMINAL_FAILURE_RATE=0.0
```

- [ ] **Step 4: Create `LICENSE` (MIT)**

Write `LICENSE`:
```
MIT License

Copyright (c) 2026 Dzmitry Martavoi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 5: Verify and commit**

Run:
```bash
ls -la
```
Expected: `.gitignore`, `.env.example`, `LICENSE`, `go.mod`, `docs/`.

Run:
```bash
git add .gitignore .env.example LICENSE go.mod
git commit -m "chore: bootstrap subflow repo (go.mod, license, env, gitignore)"
```

---

## Task 2: Proto contracts and buf codegen

**Files:**
- Create: `api/v1/subflow.proto`
- Create: `api/v1/integration.proto`
- Create: `buf.yaml`
- Create: `buf.gen.yaml`
- Create: `api/v1/*.pb.go`, `api/v1/*_grpc.pb.go` (generated)

- [ ] **Step 1: Install buf locally if not present**

Run:
```bash
which buf || brew install bufbuild/buf/buf
```
Expected: `buf` binary on PATH.

- [ ] **Step 2: Create `buf.yaml`**

Write `buf.yaml`:
```yaml
version: v2
modules:
  - path: api
lint:
  use:
    - DEFAULT
breaking:
  use:
    - FILE
```

- [ ] **Step 3: Create `buf.gen.yaml`**

Write `buf.gen.yaml`:
```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go:v1.34.2
    out: api
    opt:
      - paths=source_relative
  - remote: buf.build/grpc/go:v1.5.1
    out: api
    opt:
      - paths=source_relative
      - require_unimplemented_servers=false
```

- [ ] **Step 4: Write `api/v1/subflow.proto`**

```protobuf
syntax = "proto3";

package subflow.v1;

option go_package = "github.com/martavoi/subflow/api/v1;subflowv1";

import "google/protobuf/timestamp.proto";

service SubflowService {
  rpc CreatePlan(CreatePlanRequest) returns (Plan);
  rpc GetPlan(GetPlanRequest) returns (Plan);
  rpc ListPlans(ListPlansRequest) returns (ListPlansResponse);
  rpc DeletePlan(DeletePlanRequest) returns (DeletePlanResponse);

  rpc CreateSubscription(CreateSubscriptionRequest) returns (Subscription);
  rpc CancelSubscription(CancelSubscriptionRequest) returns (CancelSubscriptionResponse);
  rpc GetSubscription(GetSubscriptionRequest) returns (Subscription);
  rpc ListSubscriptions(ListSubscriptionsRequest) returns (ListSubscriptionsResponse);
}

message Plan {
  string id = 1;
  string code = 2;
  string name = 3;
  string billing_interval = 4; // Go duration syntax: "30s", "5m", "720h"
  int64 price_cents = 5;
  string integration_endpoint = 6;
}

message CreatePlanRequest {
  string code = 1;
  string name = 2;
  string billing_interval = 3;
  int64 price_cents = 4;
  string integration_endpoint = 5;
}

message GetPlanRequest { string id = 1; }
message ListPlansRequest {}
message ListPlansResponse { repeated Plan plans = 1; }
message DeletePlanRequest { string id = 1; }
message DeletePlanResponse {}

message Subscription {
  string id = 1;
  string user_id = 2;
  string plan_id = 3;
  string phase = 4;
  google.protobuf.Timestamp period_start = 5;
  google.protobuf.Timestamp period_end = 6;
  int32 renewal_count = 7;
  map<string, string> context = 8;
  bool cancel_requested = 9;
}

message CreateSubscriptionRequest {
  string user_id = 1;
  string plan_id = 2;
  map<string, string> initial_context = 3;
}

message CancelSubscriptionRequest { string id = 1; }
message CancelSubscriptionResponse {}

message GetSubscriptionRequest { string id = 1; }

message ListSubscriptionsRequest {
  string user_id = 1; // optional filter
  string phase = 2;   // optional filter
}
message ListSubscriptionsResponse { repeated Subscription subscriptions = 1; }
```

- [ ] **Step 5: Write `api/v1/integration.proto`**

```protobuf
syntax = "proto3";

package subflow.integration.v1;

option go_package = "github.com/martavoi/subflow/api/v1;subflowv1";

service IntegrationService {
  rpc HandleEvent(IntegrationEvent) returns (IntegrationResponse);
}

message IntegrationEvent {
  string reference = 1;          // idempotency token
  string event_type = 2;         // "subscription.activate" | "subscription.renew" | "subscription.deactivate"
  string user_id = 3;
  string plan_code = 4;
  map<string, string> context = 5;
}

message IntegrationResponse {
  map<string, string> updated_context = 1;
}
```

- [ ] **Step 6: Generate Go code**

Run from `~/Projects/subflow`:
```bash
buf generate
```
Expected: creates `api/v1/subflow.pb.go`, `api/v1/subflow_grpc.pb.go`, `api/v1/integration.pb.go`, `api/v1/integration_grpc.pb.go`.

- [ ] **Step 7: Add gRPC + protobuf deps**

Run:
```bash
go get google.golang.org/grpc@latest google.golang.org/protobuf@latest
go mod tidy
```
Expected: `go.sum` populated; `go.mod` lists the deps.

- [ ] **Step 8: Commit**

```bash
git add buf.yaml buf.gen.yaml api/v1 go.mod go.sum
git commit -m "feat: add proto contracts and buf codegen for subflow + integration services"
```

---

## Task 3: Domain types — Plan, SubscriptionInput, BillingPeriod, SubscriptionContext

**Files:**
- Create: `internal/domain/plan/plan.go`
- Create: `internal/domain/subscription/input.go`
- Create: `internal/domain/subscription/context.go`
- Create: `internal/domain/subscription/period.go`
- Create: `internal/domain/subscription/period_test.go`

- [ ] **Step 1: Create `internal/domain/plan/plan.go`**

```go
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
```

- [ ] **Step 2: Create `internal/domain/subscription/input.go`**

```go
package subscription

import "time"

// SubscriptionInput is the workflow input. Carried across Continue-As-New
// so the next run can resume cleanly.
type SubscriptionInput struct {
	SubscriptionID  string
	UserID          string
	PlanID          string
	PlanCode        string
	BillingInterval time.Duration
	IntegrationHost string
	PriceCents      int64
	PeriodStart     time.Time
	PeriodEnd       time.Time
	Context         Context
	RenewalCount    int
	CancelRequested bool
}

// IsActivation reports whether this run represents the first billing period
// of the subscription.
func (in SubscriptionInput) IsActivation() bool {
	return in.RenewalCount == 0
}
```

- [ ] **Step 3: Create `internal/domain/subscription/context.go`**

```go
package subscription

// Context is the per-subscription mutable key-value bag exchanged with the
// integration service across each lifecycle action. Mirrors the contract
// of the upstream subscription service.
type Context map[string]string

// Clone returns an independent copy of the context (workflows should never
// share map references between runs).
func (c Context) Clone() Context {
	out := make(Context, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}
```

- [ ] **Step 4: Write the failing test for `NextBillingPeriod`**

Create `internal/domain/subscription/period_test.go`:
```go
package subscription

import (
	"testing"
	"time"
)

func TestNextBillingPeriod_AdvancesByBillingInterval(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := SubscriptionInput{
		SubscriptionID:  "sub-1",
		PlanID:          "plan-1",
		BillingInterval: 30 * 24 * time.Hour,
		PeriodStart:     start,
		PeriodEnd:       start.Add(30 * 24 * time.Hour),
		RenewalCount:    0,
		Context:         Context{"k": "v"},
	}

	next := NextBillingPeriod(in)

	if got, want := next.PeriodStart, in.PeriodEnd; !got.Equal(want) {
		t.Fatalf("PeriodStart = %v, want %v", got, want)
	}
	if got, want := next.PeriodEnd, in.PeriodEnd.Add(in.BillingInterval); !got.Equal(want) {
		t.Fatalf("PeriodEnd = %v, want %v", got, want)
	}
	if got, want := next.RenewalCount, in.RenewalCount+1; got != want {
		t.Fatalf("RenewalCount = %d, want %d", got, want)
	}
	if next.CancelRequested {
		t.Fatalf("CancelRequested should never carry forward into next period")
	}
	if got, want := next.SubscriptionID, in.SubscriptionID; got != want {
		t.Fatalf("SubscriptionID = %q, want %q", got, want)
	}
}

func TestNextBillingPeriod_PreservesIdentityFields(t *testing.T) {
	in := SubscriptionInput{
		SubscriptionID:  "sub-2",
		UserID:          "user-1",
		PlanID:          "plan-1",
		PlanCode:        "monthly",
		BillingInterval: time.Hour,
		IntegrationHost: "mock:50052",
		PriceCents:      999,
		PeriodStart:     time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:       time.Date(2026, 5, 1, 1, 0, 0, 0, time.UTC),
		Context:         Context{"card_id": "card_001"},
		RenewalCount:    3,
	}

	next := NextBillingPeriod(in)

	if next.UserID != in.UserID {
		t.Fatalf("UserID lost across period roll")
	}
	if next.PlanID != in.PlanID || next.PlanCode != in.PlanCode {
		t.Fatalf("Plan identifiers lost across period roll")
	}
	if next.IntegrationHost != in.IntegrationHost {
		t.Fatalf("IntegrationHost lost across period roll")
	}
	if next.PriceCents != in.PriceCents {
		t.Fatalf("PriceCents lost across period roll")
	}
	if got, want := next.Context["card_id"], in.Context["card_id"]; got != want {
		t.Fatalf("Context lost: got %q, want %q", got, want)
	}
}

func TestNextBillingPeriod_ContextIsCloned(t *testing.T) {
	in := SubscriptionInput{
		BillingInterval: time.Hour,
		PeriodEnd:       time.Now().Add(time.Hour),
		Context:         Context{"k": "v"},
	}

	next := NextBillingPeriod(in)
	next.Context["k"] = "mutated"

	if in.Context["k"] != "v" {
		t.Fatalf("mutating next.Context leaked back to input.Context")
	}
}
```

- [ ] **Step 5: Run the test (should fail — `NextBillingPeriod` not defined)**

Run:
```bash
go test ./internal/domain/subscription/...
```
Expected: compile failure mentioning `undefined: NextBillingPeriod`.

- [ ] **Step 6: Implement `NextBillingPeriod`**

Create `internal/domain/subscription/period.go`:
```go
package subscription

// NextBillingPeriod returns a SubscriptionInput for the period immediately
// following `current`. It is a pure function (no time.Now, no randomness)
// so it is safe to call from workflow code.
func NextBillingPeriod(current SubscriptionInput) SubscriptionInput {
	return SubscriptionInput{
		SubscriptionID:  current.SubscriptionID,
		UserID:          current.UserID,
		PlanID:          current.PlanID,
		PlanCode:        current.PlanCode,
		BillingInterval: current.BillingInterval,
		IntegrationHost: current.IntegrationHost,
		PriceCents:      current.PriceCents,
		PeriodStart:     current.PeriodEnd,
		PeriodEnd:       current.PeriodEnd.Add(current.BillingInterval),
		Context:         current.Context.Clone(),
		RenewalCount:    current.RenewalCount + 1,
		CancelRequested: false,
	}
}
```

- [ ] **Step 7: Re-run tests**

Run:
```bash
go test ./internal/domain/subscription/... -v
```
Expected: all three tests pass.

- [ ] **Step 8: Commit**

```bash
git add internal/domain
git commit -m "feat(domain): plan + subscription value objects with NextBillingPeriod"
```

---

## Task 4: Mock integration service

**Files:**
- Create: `cmd/mock-integration/main.go`

This service is independent of everything else and easy to demo standalone. Build it now so subsequent activity work has a real target.

- [ ] **Step 1: Write `cmd/mock-integration/main.go`**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	subflowv1.UnimplementedIntegrationServiceServer

	failureRate         float64
	terminalFailureRate float64
	latency             time.Duration
	logger              *slog.Logger

	mu    sync.Mutex
	cache map[string]*subflowv1.IntegrationResponse // reference -> cached response
}

func (s *server) HandleEvent(ctx context.Context, ev *subflowv1.IntegrationEvent) (*subflowv1.IntegrationResponse, error) {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}

	s.mu.Lock()
	if cached, ok := s.cache[ev.Reference]; ok {
		s.mu.Unlock()
		s.logger.Info("idempotency cache hit", "reference", ev.Reference)
		return cached, nil
	}
	s.mu.Unlock()

	// Inject failures (only on first attempt; cached responses bypass).
	r := rand.Float64()
	switch {
	case r < s.terminalFailureRate:
		s.logger.Warn("injecting terminal failure", "reference", ev.Reference)
		return nil, status.Error(codes.FailedPrecondition, "injected terminal failure")
	case r < s.terminalFailureRate+s.failureRate:
		s.logger.Warn("injecting transient failure", "reference", ev.Reference)
		return nil, status.Error(codes.Unavailable, "injected transient failure")
	}

	out := &subflowv1.IntegrationResponse{UpdatedContext: map[string]string{}}
	for k, v := range ev.Context {
		out.UpdatedContext[k] = v
	}
	out.UpdatedContext["last_event"] = ev.EventType
	out.UpdatedContext["last_handled_at"] = time.Now().UTC().Format(time.RFC3339)

	s.mu.Lock()
	s.cache[ev.Reference] = out
	s.mu.Unlock()

	s.logger.Info("handled event", "reference", ev.Reference, "type", ev.EventType, "user", ev.UserId)
	return out, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := getenv("MOCK_GRPC_PORT", "50052")
	failureRate := mustFloat(getenv("FAILURE_RATE", "0.3"))
	terminalRate := mustFloat(getenv("TERMINAL_FAILURE_RATE", "0.0"))
	latencyMs := mustInt(getenv("LATENCY_MS", "0"))

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	s := &server{
		failureRate:         failureRate,
		terminalFailureRate: terminalRate,
		latency:             time.Duration(latencyMs) * time.Millisecond,
		logger:              logger,
		cache:               make(map[string]*subflowv1.IntegrationResponse),
	}

	g := grpc.NewServer()
	subflowv1.RegisterIntegrationServiceServer(g, s)

	logger.Info("mock-integration listening", "port", port,
		"failure_rate", failureRate, "terminal_failure_rate", terminalRate, "latency_ms", latencyMs)
	if err := g.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic(fmt.Errorf("parse float %q: %w", s, err))
	}
	return f
}

func mustInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Errorf("parse int %q: %w", s, err))
	}
	return n
}
```

- [ ] **Step 2: Build it**

Run:
```bash
go build -o bin/mock-integration ./cmd/mock-integration
```
Expected: `bin/mock-integration` produced.

- [ ] **Step 3: Smoke-test it**

In one terminal:
```bash
MOCK_GRPC_PORT=50052 FAILURE_RATE=0 ./bin/mock-integration
```

In another:
```bash
grpcurl -plaintext -d '{"reference":"r1","event_type":"subscription.activate","user_id":"u1","plan_code":"monthly","context":{"k":"v"}}' \
  localhost:50052 subflow.integration.v1.IntegrationService/HandleEvent
```

Expected: response contains `updated_context` with `last_event=subscription.activate` and the original `k=v`.

Run a second time with the same `reference`: response should be identical (cache hit logged in server).

Stop the server (`Ctrl+C`).

- [ ] **Step 4: Commit**

```bash
git add cmd/mock-integration
git commit -m "feat(mock-integration): gRPC server with failure/latency knobs and idempotency cache"
```

---

## Task 5: Mongo store — connection, plans, projection

**Files:**
- Create: `internal/store/mongo.go`
- Create: `internal/store/plans.go`
- Create: `internal/store/projection.go`

- [ ] **Step 1: Add Mongo driver dep**

Run:
```bash
go get go.mongodb.org/mongo-driver/v2/mongo@latest
go mod tidy
```

- [ ] **Step 2: Create `internal/store/mongo.go`**

```go
package store

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Connect dials Mongo and returns a *mongo.Database for the given database name.
func Connect(ctx context.Context, uri, database string) (*mongo.Client, *mongo.Database, error) {
	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, nil, fmt.Errorf("mongo ping: %w", err)
	}
	return client, client.Database(database), nil
}
```

- [ ] **Step 3: Create `internal/store/plans.go`**

```go
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/domain/plan"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrPlanNotFound = errors.New("plan not found")

type planDoc struct {
	ID                  string    `bson:"_id"`
	Code                string    `bson:"code"`
	Name                string    `bson:"name"`
	BillingInterval     string    `bson:"billing_interval"` // stored as Go duration string for human-readable docs
	PriceCents          int64     `bson:"price_cents"`
	IntegrationEndpoint string    `bson:"integration_endpoint"`
	CreatedAt           time.Time `bson:"created_at"`
}

type PlanRepository struct {
	col *mongo.Collection
}

func NewPlanRepository(db *mongo.Database) *PlanRepository {
	return &PlanRepository{col: db.Collection("plans")}
}

// EnsureIndexes creates required indexes. Idempotent.
func (r *PlanRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "code", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("plans_code_unique"),
	})
	return err
}

func (r *PlanRepository) Insert(ctx context.Context, p plan.Plan) error {
	_, err := r.col.InsertOne(ctx, planDoc{
		ID:                  p.ID,
		Code:                p.Code,
		Name:                p.Name,
		BillingInterval:     p.BillingInterval.String(),
		PriceCents:          p.PriceCents,
		IntegrationEndpoint: p.IntegrationEndpoint,
		CreatedAt:           p.CreatedAt,
	})
	return err
}

func (r *PlanRepository) Get(ctx context.Context, id string) (plan.Plan, error) {
	var d planDoc
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return plan.Plan{}, ErrPlanNotFound
	}
	if err != nil {
		return plan.Plan{}, err
	}
	return docToPlan(d)
}

func (r *PlanRepository) List(ctx context.Context) ([]plan.Plan, error) {
	cur, err := r.col.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]plan.Plan, 0)
	for cur.Next(ctx) {
		var d planDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		p, err := docToPlan(d)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, cur.Err()
}

func (r *PlanRepository) Delete(ctx context.Context, id string) error {
	res, err := r.col.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return ErrPlanNotFound
	}
	return nil
}

func docToPlan(d planDoc) (plan.Plan, error) {
	dur, err := time.ParseDuration(d.BillingInterval)
	if err != nil {
		return plan.Plan{}, fmt.Errorf("parse billing_interval %q: %w", d.BillingInterval, err)
	}
	return plan.Plan{
		ID:                  d.ID,
		Code:                d.Code,
		Name:                d.Name,
		BillingInterval:     dur,
		PriceCents:          d.PriceCents,
		IntegrationEndpoint: d.IntegrationEndpoint,
		CreatedAt:           d.CreatedAt,
	}, nil
}
```

- [ ] **Step 4: Create `internal/store/projection.go`**

```go
package store

import (
	"context"
	"errors"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrSubscriptionNotFound = errors.New("subscription not found")

// SubscriptionView is the read-model projection record.
type SubscriptionView struct {
	ID              string               `bson:"_id"`
	UserID          string               `bson:"user_id"`
	PlanID          string               `bson:"plan_id"`
	Phase           string               `bson:"phase"`
	PeriodStart     time.Time            `bson:"period_start"`
	PeriodEnd       time.Time            `bson:"period_end"`
	RenewalCount    int                  `bson:"renewal_count"`
	Context         subscription.Context `bson:"context"`
	CancelRequested bool                 `bson:"cancel_requested"`
	UpdatedAt       time.Time            `bson:"updated_at"`
}

type SubscriptionProjectionRepository struct {
	col *mongo.Collection
}

func NewSubscriptionProjectionRepository(db *mongo.Database) *SubscriptionProjectionRepository {
	return &SubscriptionProjectionRepository{col: db.Collection("subscriptions_view")}
}

func (r *SubscriptionProjectionRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "user_id", Value: 1}}, Options: options.Index().SetName("user_id_idx")},
		{Keys: bson.D{{Key: "phase", Value: 1}}, Options: options.Index().SetName("phase_idx")},
	})
	return err
}

func (r *SubscriptionProjectionRepository) Upsert(ctx context.Context, v SubscriptionView) error {
	v.UpdatedAt = time.Now().UTC()
	_, err := r.col.ReplaceOne(ctx,
		bson.M{"_id": v.ID}, v,
		options.Replace().SetUpsert(true),
	)
	return err
}

func (r *SubscriptionProjectionRepository) Get(ctx context.Context, id string) (SubscriptionView, error) {
	var v SubscriptionView
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&v)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return SubscriptionView{}, ErrSubscriptionNotFound
	}
	return v, err
}

func (r *SubscriptionProjectionRepository) List(ctx context.Context, userID, phase string) ([]SubscriptionView, error) {
	filter := bson.M{}
	if userID != "" {
		filter["user_id"] = userID
	}
	if phase != "" {
		filter["phase"] = phase
	}
	cur, err := r.col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]SubscriptionView, 0)
	for cur.Next(ctx) {
		var v SubscriptionView
		if err := cur.Decode(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, cur.Err()
}
```

- [ ] **Step 5: Verify it compiles**

Run:
```bash
go build ./internal/store/...
```
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/store
git commit -m "feat(store): mongo connection, plan repository, subscription projection repository"
```

---

## Task 6: Activity scaffolding — error types and retry policies

**Files:**
- Create: `internal/activity/errors.go`
- Create: `internal/activity/retry.go`

- [ ] **Step 1: Add Temporal SDK dep**

Run:
```bash
go get go.temporal.io/sdk@latest
go mod tidy
```

- [ ] **Step 2: Create `internal/activity/errors.go`**

```go
package activity

// Error type names used to classify failures. Names listed in a RetryPolicy's
// NonRetryableErrorTypes match against ApplicationError.Type values exactly.
const (
	ErrTypeInsufficientFunds   = "InsufficientFundsError"
	ErrTypeCardDeclined        = "CardDeclinedError"
	ErrTypeIntegrationTerminal = "IntegrationTerminalError"
)
```

- [ ] **Step 3: Create `internal/activity/retry.go`**

```go
package activity

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// PaymentRetry retries transient payment failures with backoff but stops on
// known terminal billing errors (declined, insufficient funds).
var PaymentRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    5,
	NonRetryableErrorTypes: []string{
		ErrTypeInsufficientFunds,
		ErrTypeCardDeclined,
	},
}

// EventPublishingRetry retries forever — events should eventually publish.
// Operator can fix the bus and let it drain.
var EventPublishingRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 1.5,
	MaximumInterval:    30 * time.Second,
	MaximumAttempts:    0,
}

// IntegrationCallRetry retries forever for transient gRPC failures and stops
// only on integration-side terminal errors.
var IntegrationCallRetry = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute,
	MaximumAttempts:    0,
	NonRetryableErrorTypes: []string{
		ErrTypeIntegrationTerminal,
	},
}
```

- [ ] **Step 4: Verify**

Run:
```bash
go build ./internal/activity/...
```
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum internal/activity
git commit -m "feat(activity): named retry policies and error type names"
```

---

## Task 7: Activity — `ChargePayment`

**Files:**
- Create: `internal/activity/payment.go`

- [ ] **Step 1: Create `internal/activity/payment.go`**

```go
package activity

import (
	"context"
	"log/slog"
	"math/rand/v2"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// ChargePaymentInput is the activity input. Reference is the idempotency token
// constructed by the workflow.
type ChargePaymentInput struct {
	Reference  string
	UserID     string
	PriceCents int64
}

type ChargePaymentResult struct {
	Reference     string
	TransactionID string
	AmountCents   int64
}

// PaymentActivities holds the (mocked) configuration for charging payments.
// In a real implementation this would hold a payment gateway client.
type PaymentActivities struct {
	TransientFailureRate float64
	TerminalFailureRate  float64
}

// ChargePayment is the registered activity. It simulates a payment charge
// with configurable failure injection.
func (a *PaymentActivities) ChargePayment(ctx context.Context, in ChargePaymentInput) (ChargePaymentResult, error) {
	logger := activity.GetLogger(ctx)

	r := rand.Float64()
	switch {
	case r < a.TerminalFailureRate:
		logger.Warn("ChargePayment terminal failure (declined)", slog.String("ref", in.Reference))
		return ChargePaymentResult{}, temporal.NewNonRetryableApplicationError(
			"card declined", ErrTypeCardDeclined, nil)
	case r < a.TerminalFailureRate+a.TransientFailureRate:
		logger.Warn("ChargePayment transient failure", slog.String("ref", in.Reference))
		return ChargePaymentResult{}, temporal.NewApplicationError(
			"payment gateway timeout", "PaymentGatewayTimeoutError")
	}

	logger.Info("ChargePayment success",
		slog.String("ref", in.Reference),
		slog.String("user", in.UserID),
		slog.Int64("cents", in.PriceCents))

	return ChargePaymentResult{
		Reference:     in.Reference,
		TransactionID: "txn-" + in.Reference,
		AmountCents:   in.PriceCents,
	}, nil
}
```

- [ ] **Step 2: Verify**

Run:
```bash
go build ./internal/activity/...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/activity/payment.go
git commit -m "feat(activity): ChargePayment with retryable + non-retryable failure modes"
```

---

## Task 8: Activity — `PublishSubscriptionEvent` + eventbus stub

**Files:**
- Create: `internal/eventbus/stub.go`
- Create: `internal/activity/events.go`

- [ ] **Step 1: Create `internal/eventbus/stub.go`**

```go
package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
)

// Event is the canonical lifecycle event payload published to downstream
// consumers. Stable shape so swapping the stub for Kafka later is a one-file
// change.
type Event struct {
	Reference      string               `json:"reference"`
	Type           string               `json:"type"`
	SubscriptionID string               `json:"subscription_id"`
	UserID         string               `json:"user_id"`
	PlanID         string               `json:"plan_id"`
	PlanCode       string               `json:"plan_code"`
	PeriodStart    time.Time            `json:"period_start"`
	PeriodEnd      time.Time            `json:"period_end"`
	RenewalCount   int                  `json:"renewal_count"`
	Context        subscription.Context `json:"context"`
	OccurredAt     time.Time            `json:"occurred_at"`
}

// Publisher is the swap point. Stub writes to stdout; a real Kafka publisher
// would implement this interface.
type Publisher interface {
	Publish(ctx context.Context, ev Event) error
}

// StdoutPublisher writes events to stdout as one JSON object per line.
type StdoutPublisher struct {
	Logger *slog.Logger
}

func NewStdoutPublisher(logger *slog.Logger) *StdoutPublisher {
	return &StdoutPublisher{Logger: logger}
}

func (p *StdoutPublisher) Publish(_ context.Context, ev Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "subflow.event "+string(b)); err != nil {
		return err
	}
	if p.Logger != nil {
		p.Logger.Info("event published", slog.String("type", ev.Type), slog.String("ref", ev.Reference))
	}
	return nil
}
```

- [ ] **Step 2: Create `internal/activity/events.go`**

```go
package activity

import (
	"context"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/eventbus"
)

const (
	EventTypeActivate   = "subscription.activate"
	EventTypeRenew      = "subscription.renew"
	EventTypeDeactivate = "subscription.deactivate"
)

type PublishEventInput struct {
	Reference      string
	EventType      string
	SubscriptionID string
	UserID         string
	PlanID         string
	PlanCode       string
	PeriodStart    time.Time
	PeriodEnd      time.Time
	RenewalCount   int
	Context        subscription.Context
}

type EventActivities struct {
	Publisher eventbus.Publisher
	Now       func() time.Time // injectable for tests; defaults to time.Now in main
}

func (a *EventActivities) PublishSubscriptionEvent(ctx context.Context, in PublishEventInput) error {
	now := a.Now
	if now == nil {
		now = time.Now
	}
	ev := eventbus.Event{
		Reference:      in.Reference,
		Type:           in.EventType,
		SubscriptionID: in.SubscriptionID,
		UserID:         in.UserID,
		PlanID:         in.PlanID,
		PlanCode:       in.PlanCode,
		PeriodStart:    in.PeriodStart,
		PeriodEnd:      in.PeriodEnd,
		RenewalCount:   in.RenewalCount,
		Context:        in.Context,
		OccurredAt:     now().UTC(),
	}
	return a.Publisher.Publish(ctx, ev)
}
```

- [ ] **Step 3: Verify**

Run:
```bash
go build ./internal/activity/... ./internal/eventbus/...
```
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/eventbus internal/activity/events.go
git commit -m "feat(activity): PublishSubscriptionEvent + stdout-backed eventbus stub"
```

---

## Task 9: Activity — `NotifyIntegrationService` + integration client

**Files:**
- Create: `internal/integration/client.go`
- Create: `internal/activity/integration.go`

- [ ] **Step 1: Create `internal/integration/client.go`**

```go
package integration

import (
	"context"
	"fmt"
	"sync"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps a lazily-dialed pool of gRPC connections to integration
// endpoints. Each plan can specify its own endpoint.
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewClient() *Client {
	return &Client{conns: make(map[string]*grpc.ClientConn)}
}

func (c *Client) Stub(endpoint string) (subflowv1.IntegrationServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[endpoint]; ok {
		return subflowv1.NewIntegrationServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %q: %w", endpoint, err)
	}
	c.conns[endpoint] = conn
	return subflowv1.NewIntegrationServiceClient(conn), nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// HandleEvent calls the IntegrationService at the given endpoint with retries
// disabled (Temporal handles retries via activity options).
func (c *Client) HandleEvent(ctx context.Context, endpoint string, ev *subflowv1.IntegrationEvent) (*subflowv1.IntegrationResponse, error) {
	stub, err := c.Stub(endpoint)
	if err != nil {
		return nil, err
	}
	return stub.HandleEvent(ctx, ev)
}
```

- [ ] **Step 2: Create `internal/activity/integration.go`**

```go
package activity

import (
	"context"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/integration"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IntegrationCallInput struct {
	Reference       string
	EventType       string
	UserID          string
	PlanCode        string
	IntegrationHost string
	Context         subscription.Context
}

type IntegrationCallResult struct {
	UpdatedContext subscription.Context
}

type IntegrationActivities struct {
	Client *integration.Client
}

func (a *IntegrationActivities) NotifyIntegrationService(ctx context.Context, in IntegrationCallInput) (IntegrationCallResult, error) {
	resp, err := a.Client.HandleEvent(ctx, in.IntegrationHost, &subflowv1.IntegrationEvent{
		Reference: in.Reference,
		EventType: in.EventType,
		UserId:    in.UserID,
		PlanCode:  in.PlanCode,
		Context:   map[string]string(in.Context),
	})
	if err != nil {
		// Map gRPC status codes to Temporal error semantics.
		// FailedPrecondition / InvalidArgument / NotFound -> non-retryable terminal.
		// Everything else (Unavailable, DeadlineExceeded, Unknown) -> retryable.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.FailedPrecondition, codes.InvalidArgument, codes.NotFound:
				return IntegrationCallResult{}, temporal.NewNonRetryableApplicationError(
					st.Message(), ErrTypeIntegrationTerminal, err)
			}
		}
		return IntegrationCallResult{}, temporal.NewApplicationError(
			err.Error(), "IntegrationTransientError")
	}

	return IntegrationCallResult{
		UpdatedContext: subscription.Context(resp.UpdatedContext),
	}, nil
}
```

- [ ] **Step 3: Verify**

Run:
```bash
go build ./internal/integration/... ./internal/activity/...
```
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/integration internal/activity/integration.go
git commit -m "feat(activity): NotifyIntegrationService with gRPC client and error mapping"
```

---

## Task 10: Activity — `UpdateSubscriptionProjection`

**Files:**
- Create: `internal/activity/projection.go`

- [ ] **Step 1: Create `internal/activity/projection.go`**

```go
package activity

import (
	"context"
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/store"
)

const (
	PhasePending     = "pending"
	PhaseActive      = "active"
	PhaseCancelling  = "cancelling"
	PhaseDeactivated = "deactivated"
)

type ProjectionUpdate struct {
	SubscriptionID  string
	UserID          string
	PlanID          string
	Phase           string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool
}

type ProjectionActivities struct {
	Repo *store.SubscriptionProjectionRepository
}

func (a *ProjectionActivities) UpdateSubscriptionProjection(ctx context.Context, u ProjectionUpdate) error {
	return a.Repo.Upsert(ctx, store.SubscriptionView{
		ID:              u.SubscriptionID,
		UserID:          u.UserID,
		PlanID:          u.PlanID,
		Phase:           u.Phase,
		PeriodStart:     u.PeriodStart,
		PeriodEnd:       u.PeriodEnd,
		RenewalCount:    u.RenewalCount,
		Context:         u.Context,
		CancelRequested: u.CancelRequested,
	})
}
```

- [ ] **Step 2: Verify**

Run:
```bash
go build ./internal/activity/...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/activity/projection.go
git commit -m "feat(activity): UpdateSubscriptionProjection writes read-model row"
```

---

## Task 11: Workflow scaffolding — signals, state, query handler

**Files:**
- Create: `internal/workflow/signals.go`
- Create: `internal/workflow/state.go`

- [ ] **Step 1: Create `internal/workflow/signals.go`**

```go
package workflow

// Signal and query names for SubscriptionWorkflow. Stable strings — clients
// reference these directly.
const (
	SignalCancelSubscription = "subscription.cancel"
	QuerySubscriptionStatus  = "subscription.status"
)
```

- [ ] **Step 2: Create `internal/workflow/state.go`**

```go
package workflow

import (
	"time"

	"github.com/martavoi/subflow/internal/domain/subscription"
)

// SubscriptionStatus is the snapshot returned by the QuerySubscriptionStatus
// query handler. Mirrors the read-model fields the API needs.
type SubscriptionStatus struct {
	Phase           string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	RenewalCount    int
	Context         subscription.Context
	CancelRequested bool
}

// SubscriptionState is the in-memory state mutated during a single workflow
// run. It is recreated from input + replay on every run; not persisted.
type SubscriptionState struct {
	Input subscription.SubscriptionInput
	Phase string
}

// AsStatus returns the queryable snapshot.
func (s *SubscriptionState) AsStatus() (SubscriptionStatus, error) {
	return SubscriptionStatus{
		Phase:           s.Phase,
		PeriodStart:     s.Input.PeriodStart,
		PeriodEnd:       s.Input.PeriodEnd,
		RenewalCount:    s.Input.RenewalCount,
		Context:         s.Input.Context.Clone(),
		CancelRequested: s.Input.CancelRequested,
	}, nil
}
```

- [ ] **Step 3: Verify**

Run:
```bash
go build ./internal/workflow/...
```
Expected: no errors (workflow file may not exist yet — that's fine, ignore "no Go files" if you see it; verify no compile errors instead by running `go build ./...` after Task 13).

- [ ] **Step 4: Commit**

```bash
git add internal/workflow/signals.go internal/workflow/state.go
git commit -m "feat(workflow): signal/query name constants + SubscriptionState"
```

---

## Task 12: Workflow lifecycle verbs

**Files:**
- Create: `internal/workflow/lifecycle.go`

- [ ] **Step 1: Create `internal/workflow/lifecycle.go`**

```go
package workflow

import (
	"fmt"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// activityRef constructs an idempotency token for an activity call.
// Stable across retries within a run, unique across runs (run ID changes).
func activityRef(ctx workflow.Context, suffix string) string {
	info := workflow.GetInfo(ctx)
	return fmt.Sprintf("%s:%s:%s", info.WorkflowExecution.ID, info.WorkflowExecution.RunID, suffix)
}

// StartBillingPeriod dispatches to activation or renewal based on whether this
// is the first billing period.
func StartBillingPeriod(ctx workflow.Context, state *SubscriptionState) error {
	if state.Input.IsActivation() {
		return ActivateSubscription(ctx, state)
	}
	return RenewSubscription(ctx, state)
}

// ActivateSubscription runs the period-start activities for the very first
// billing period: charge → publish → notify integration → project.
func ActivateSubscription(ctx workflow.Context, state *SubscriptionState) error {
	state.Phase = activity.PhasePending
	if err := updateProjection(ctx, state); err != nil {
		return err
	}
	if err := chargeAndPublish(ctx, state, activity.EventTypeActivate); err != nil {
		return err
	}
	if err := notifyIntegrationAndUpdateContext(ctx, state, activity.EventTypeActivate); err != nil {
		return err
	}
	state.Phase = activity.PhaseActive
	return updateProjection(ctx, state)
}

// RenewSubscription runs the period-start activities for a renewal period.
func RenewSubscription(ctx workflow.Context, state *SubscriptionState) error {
	if err := chargeAndPublish(ctx, state, activity.EventTypeRenew); err != nil {
		return err
	}
	if err := notifyIntegrationAndUpdateContext(ctx, state, activity.EventTypeRenew); err != nil {
		return err
	}
	state.Phase = activity.PhaseActive
	return updateProjection(ctx, state)
}

// DeactivateSubscription publishes the deactivation event, notifies the
// integration service, and writes the terminal projection.
func DeactivateSubscription(ctx workflow.Context, state *SubscriptionState) error {
	state.Phase = activity.PhaseCancelling
	if err := updateProjection(ctx, state); err != nil {
		return err
	}
	if err := publishLifecycleEvent(ctx, state, activity.EventTypeDeactivate); err != nil {
		return err
	}
	if err := notifyIntegrationAndUpdateContext(ctx, state, activity.EventTypeDeactivate); err != nil {
		return err
	}
	state.Phase = activity.PhaseDeactivated
	return updateProjection(ctx, state)
}

// AwaitPeriodEndOrCancellation parks the workflow until the period timer
// fires or a cancel signal is received. End-of-period semantics: if a cancel
// arrives early, sleep the remainder of the period before returning.
//
// Returns true if the subscription was cancelled.
func AwaitPeriodEndOrCancellation(ctx workflow.Context, state *SubscriptionState) bool {
	cancelCh := workflow.GetSignalChannel(ctx, SignalCancelSubscription)
	cancelled := state.Input.CancelRequested

	now := workflow.Now(ctx)
	if state.Input.PeriodEnd.After(now) {
		timer := workflow.NewTimer(ctx, state.Input.PeriodEnd.Sub(now))
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(timer, func(workflow.Future) {})
		sel.AddReceive(cancelCh, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			cancelled = true
			state.Input.CancelRequested = true
			state.Phase = activity.PhaseCancelling
			_ = updateProjection(ctx, state)
		})
		sel.Select(ctx)
	}

	// Honor end-of-period: if cancel arrived early, sleep the remainder.
	if cancelled {
		remaining := state.Input.PeriodEnd.Sub(workflow.Now(ctx))
		if remaining > 0 {
			_ = workflow.Sleep(ctx, remaining)
		}
	}
	return cancelled
}

// ContinueIntoNextPeriod restarts the workflow as a new run for the next
// billing period (Continue-As-New per renewal).
func ContinueIntoNextPeriod(ctx workflow.Context, state *SubscriptionState) error {
	return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow,
		subscription.NextBillingPeriod(state.Input))
}

// --- helpers below — kept private intentionally because they are pure
// orchestration glue, not domain verbs. Each domain verb above remains the
// public, named, testable surface.

func chargeAndPublish(ctx workflow.Context, state *SubscriptionState, eventType string) error {
	chargeIn := activity.ChargePaymentInput{
		Reference:  activityRef(ctx, "charge:"+eventType),
		UserID:     state.Input.UserID,
		PriceCents: state.Input.PriceCents,
	}
	chargeOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.PaymentRetry,
	})
	var chargeRes activity.ChargePaymentResult
	if err := workflow.ExecuteActivity(chargeOpts, "ChargePayment", chargeIn).Get(ctx, &chargeRes); err != nil {
		return err
	}
	return publishLifecycleEvent(ctx, state, eventType)
}

func publishLifecycleEvent(ctx workflow.Context, state *SubscriptionState, eventType string) error {
	pubIn := activity.PublishEventInput{
		Reference:      activityRef(ctx, "publish:"+eventType),
		EventType:      eventType,
		SubscriptionID: state.Input.SubscriptionID,
		UserID:         state.Input.UserID,
		PlanID:         state.Input.PlanID,
		PlanCode:       state.Input.PlanCode,
		PeriodStart:    state.Input.PeriodStart,
		PeriodEnd:      state.Input.PeriodEnd,
		RenewalCount:   state.Input.RenewalCount,
		Context:        state.Input.Context.Clone(),
	}
	pubOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy:         activity.EventPublishingRetry,
	})
	return workflow.ExecuteActivity(pubOpts, "PublishSubscriptionEvent", pubIn).Get(ctx, nil)
}

func notifyIntegrationAndUpdateContext(ctx workflow.Context, state *SubscriptionState, eventType string) error {
	notifyIn := activity.IntegrationCallInput{
		Reference:       activityRef(ctx, "integration:"+eventType),
		EventType:       eventType,
		UserID:          state.Input.UserID,
		PlanCode:        state.Input.PlanCode,
		IntegrationHost: state.Input.IntegrationHost,
		Context:         state.Input.Context.Clone(),
	}
	notifyOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         activity.IntegrationCallRetry,
	})
	var notifyRes activity.IntegrationCallResult
	if err := workflow.ExecuteActivity(notifyOpts, "NotifyIntegrationService", notifyIn).Get(ctx, &notifyRes); err != nil {
		return err
	}
	if notifyRes.UpdatedContext != nil {
		state.Input.Context = notifyRes.UpdatedContext
	}
	return nil
}

func updateProjection(ctx workflow.Context, state *SubscriptionState) error {
	upd := activity.ProjectionUpdate{
		SubscriptionID:  state.Input.SubscriptionID,
		UserID:          state.Input.UserID,
		PlanID:          state.Input.PlanID,
		Phase:           state.Phase,
		PeriodStart:     state.Input.PeriodStart,
		PeriodEnd:       state.Input.PeriodEnd,
		RenewalCount:    state.Input.RenewalCount,
		Context:         state.Input.Context.Clone(),
		CancelRequested: state.Input.CancelRequested,
	}
	opts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         activity.EventPublishingRetry, // same gentle retry — projection should always eventually write
	})
	return workflow.ExecuteActivity(opts, "UpdateSubscriptionProjection", upd).Get(ctx, nil)
}
```

- [ ] **Step 2: Commit (will not yet build until Task 13 adds the entry function)**

```bash
git add internal/workflow/lifecycle.go
git commit -m "feat(workflow): exported lifecycle verbs (Activate/Renew/Deactivate/Await/Continue)"
```

---

## Task 13: Workflow entry + testsuite tests

**Files:**
- Create: `internal/workflow/subscription.go`
- Create: `internal/workflow/subscription_test.go`

- [ ] **Step 1: Create `internal/workflow/subscription.go`**

```go
package workflow

import (
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/workflow"
)

// SubscriptionWorkflow is the durable subscription lifecycle. One workflow
// run per billing period; Continue-As-New advances to the next period until
// the subscription is cancelled.
//
// Workflow ID convention: "subscription:<SubscriptionID>" — addressable for
// signals and queries by the API layer.
func SubscriptionWorkflow(ctx workflow.Context, in subscription.SubscriptionInput) error {
	state := &SubscriptionState{
		Input: in,
		Phase: "starting",
	}

	if err := workflow.SetQueryHandler(ctx, QuerySubscriptionStatus, state.AsStatus); err != nil {
		return err
	}

	if err := StartBillingPeriod(ctx, state); err != nil {
		return err
	}

	cancelled := AwaitPeriodEndOrCancellation(ctx, state)
	if cancelled {
		return DeactivateSubscription(ctx, state)
	}

	return ContinueIntoNextPeriod(ctx, state)
}
```

- [ ] **Step 2: Verify the workflow package compiles**

Run:
```bash
go build ./...
```
Expected: no errors.

- [ ] **Step 3: Write the workflow test**

Note on imports: the project's `internal/activity` package and the SDK's `go.temporal.io/sdk/activity` package have the same name. We import the project package as `activityPkg` so SDK references stay unambiguous.

Create `internal/workflow/subscription_test.go`:
```go
package workflow

import (
	"testing"
	"time"

	activityPkg "github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func sampleInput() subscription.SubscriptionInput {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return subscription.SubscriptionInput{
		SubscriptionID:  "sub-1",
		UserID:          "user-1",
		PlanID:          "plan-1",
		PlanCode:        "monthly-basic",
		BillingInterval: 30 * 24 * time.Hour,
		IntegrationHost: "mock:50052",
		PriceCents:      999,
		PeriodStart:     start,
		PeriodEnd:       start.Add(30 * 24 * time.Hour),
		Context:         subscription.Context{"card_id": "card_001"},
	}
}

func TestSubscriptionWorkflow_HappyActivation_ContinuesAsNew(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	registerActivityMocks(env, nil)

	env.ExecuteWorkflow(SubscriptionWorkflow, sampleInput())

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatalf("expected ContinueAsNew error, got nil")
	}
}

func TestSubscriptionWorkflow_CancelMidPeriod_RunsDeactivation(t *testing.T) {
	s := testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()

	deactivateCalled := false
	registerActivityMocks(env, func(eventType string) {
		if eventType == activityPkg.EventTypeDeactivate {
			deactivateCalled = true
		}
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalCancelSubscription, nil)
	}, 5*24*time.Hour)

	env.ExecuteWorkflow(SubscriptionWorkflow, sampleInput())

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow not completed")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if !deactivateCalled {
		t.Fatalf("expected deactivation event to be published, none observed")
	}
}

func registerActivityMocks(env *testsuite.TestWorkflowEnvironment, onPublish func(eventType string)) {
	env.RegisterActivityWithOptions(
		func(in activityPkg.ChargePaymentInput) (activityPkg.ChargePaymentResult, error) {
			return activityPkg.ChargePaymentResult{Reference: in.Reference, TransactionID: "txn", AmountCents: in.PriceCents}, nil
		},
		activity.RegisterOptions{Name: "ChargePayment"},
	)
	env.RegisterActivityWithOptions(
		func(in activityPkg.PublishEventInput) error {
			if onPublish != nil {
				onPublish(in.EventType)
			}
			return nil
		},
		activity.RegisterOptions{Name: "PublishSubscriptionEvent"},
	)
	env.RegisterActivityWithOptions(
		func(in activityPkg.IntegrationCallInput) (activityPkg.IntegrationCallResult, error) {
			out := subscription.Context{}
			for k, v := range in.Context {
				out[k] = v
			}
			out["last_event"] = in.EventType
			return activityPkg.IntegrationCallResult{UpdatedContext: out}, nil
		},
		activity.RegisterOptions{Name: "NotifyIntegrationService"},
	)
	env.RegisterActivityWithOptions(
		func(_ activityPkg.ProjectionUpdate) error { return nil },
		activity.RegisterOptions{Name: "UpdateSubscriptionProjection"},
	)
}
```

- [ ] **Step 4: Run the tests**

Run:
```bash
go test ./internal/workflow/... -v
```
Expected: both tests PASS. The first test asserts the workflow ended with a non-nil error (Continue-As-New is reported as an error to the test harness). The second asserts cancellation triggers deactivation.

If the cancel test stalls or the time skipping behaves unexpectedly, debug by:
- Logging `env.OnActivity(...)` invocations.
- Confirming `RegisterDelayedCallback` fires before `PeriodEnd`.

- [ ] **Step 5: Commit**

```bash
git add internal/workflow/subscription.go internal/workflow/subscription_test.go
git commit -m "feat(workflow): SubscriptionWorkflow entry + lifecycle/cancel testsuite tests"
```

---

## Task 14: Config loader

**Files:**
- Create: `internal/config/config.go`

- [ ] **Step 1: Create `internal/config/config.go`**

```go
package config

import (
	"fmt"
	"os"
	"strconv"
)

type API struct {
	GRPCPort          string
	TemporalHost      string
	TemporalNamespace string
	MongoURI          string
	MongoDatabase     string
	IntegrationHost   string // default endpoint when plan does not override
}

type Worker struct {
	TemporalHost          string
	TemporalNamespace     string
	TaskQueue             string
	MongoURI              string
	MongoDatabase         string
	PaymentTransientRate  float64
	PaymentTerminalRate   float64
}

type MockIntegration struct {
	GRPCPort            string
	FailureRate         float64
	TerminalFailureRate float64
	LatencyMS           int
}

func LoadAPI() (API, error) {
	return API{
		GRPCPort:          getenv("API_GRPC_PORT", "50051"),
		TemporalHost:      getenv("TEMPORAL_HOST", "localhost:7233"),
		TemporalNamespace: getenv("TEMPORAL_NAMESPACE", "default"),
		MongoURI:          getenv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:     getenv("MONGO_DATABASE", "subflow"),
		IntegrationHost:   getenv("INTEGRATION_HOST", "localhost:50052"),
	}, nil
}

func LoadWorker() (Worker, error) {
	tr, err := parseFloat(getenv("PAYMENT_TRANSIENT_RATE", "0.0"))
	if err != nil {
		return Worker{}, err
	}
	tt, err := parseFloat(getenv("PAYMENT_TERMINAL_RATE", "0.0"))
	if err != nil {
		return Worker{}, err
	}
	return Worker{
		TemporalHost:         getenv("TEMPORAL_HOST", "localhost:7233"),
		TemporalNamespace:    getenv("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:            getenv("TASK_QUEUE", "subflow"),
		MongoURI:             getenv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:        getenv("MONGO_DATABASE", "subflow"),
		PaymentTransientRate: tr,
		PaymentTerminalRate:  tt,
	}, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func parseFloat(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q as float: %w", s, err)
	}
	return f, nil
}
```

- [ ] **Step 2: Verify**

Run:
```bash
go build ./internal/config/...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/config
git commit -m "feat(config): API, Worker, MockIntegration env-var loaders"
```

---

## Task 15: gRPC server — plan handlers

**Files:**
- Create: `internal/server/plans.go`

- [ ] **Step 1: Create `internal/server/plans.go`**

```go
package server

import (
	"context"
	"errors"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PlanService implements the plan-related RPCs on SubflowService.
type PlanService struct {
	Repo *store.PlanRepository
}

func (s *PlanService) CreatePlan(ctx context.Context, req *subflowv1.CreatePlanRequest) (*subflowv1.Plan, error) {
	dur, err := time.ParseDuration(req.BillingInterval)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "billing_interval %q is not a valid Go duration: %v", req.BillingInterval, err)
	}
	p := plan.Plan{
		ID:                  uuid.NewString(),
		Code:                req.Code,
		Name:                req.Name,
		BillingInterval:     dur,
		PriceCents:          req.PriceCents,
		IntegrationEndpoint: req.IntegrationEndpoint,
		CreatedAt:           time.Now().UTC(),
	}
	if err := s.Repo.Insert(ctx, p); err != nil {
		return nil, status.Errorf(codes.Internal, "insert plan: %v", err)
	}
	return planToProto(p), nil
}

func (s *PlanService) GetPlan(ctx context.Context, req *subflowv1.GetPlanRequest) (*subflowv1.Plan, error) {
	p, err := s.Repo.Get(ctx, req.Id)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}
	return planToProto(p), nil
}

func (s *PlanService) ListPlans(ctx context.Context, _ *subflowv1.ListPlansRequest) (*subflowv1.ListPlansResponse, error) {
	plans, err := s.Repo.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list plans: %v", err)
	}
	out := &subflowv1.ListPlansResponse{Plans: make([]*subflowv1.Plan, 0, len(plans))}
	for _, p := range plans {
		out.Plans = append(out.Plans, planToProto(p))
	}
	return out, nil
}

func (s *PlanService) DeletePlan(ctx context.Context, req *subflowv1.DeletePlanRequest) (*subflowv1.DeletePlanResponse, error) {
	if err := s.Repo.Delete(ctx, req.Id); err != nil {
		if errors.Is(err, store.ErrPlanNotFound) {
			return nil, status.Error(codes.NotFound, "plan not found")
		}
		return nil, status.Errorf(codes.Internal, "delete plan: %v", err)
	}
	return &subflowv1.DeletePlanResponse{}, nil
}

func planToProto(p plan.Plan) *subflowv1.Plan {
	return &subflowv1.Plan{
		Id:                  p.ID,
		Code:                p.Code,
		Name:                p.Name,
		BillingInterval:     p.BillingInterval.String(),
		PriceCents:          p.PriceCents,
		IntegrationEndpoint: p.IntegrationEndpoint,
	}
}
```

- [ ] **Step 2: Add UUID dep**

Run:
```bash
go get github.com/google/uuid@latest
go mod tidy
```

- [ ] **Step 3: Verify**

Run:
```bash
go build ./internal/server/...
```
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum internal/server/plans.go
git commit -m "feat(server): plan CRUD handlers"
```

---

## Task 16: gRPC server — subscription handlers

**Files:**
- Create: `internal/server/subscriptions.go`

- [ ] **Step 1: Create `internal/server/subscriptions.go`**

```go
package server

import (
	"context"
	"errors"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/domain/plan"
	"github.com/martavoi/subflow/internal/domain/subscription"
	"github.com/martavoi/subflow/internal/store"
	"github.com/martavoi/subflow/internal/workflow"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SubscriptionService translates subscription RPCs into Temporal client calls
// + projection lookups. Contains zero business logic.
type SubscriptionService struct {
	Temporal      client.Client
	TaskQueue     string
	PlanRepo      *store.PlanRepository
	Projection    *store.SubscriptionProjectionRepository
	DefaultIntegration string
}

func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *subflowv1.CreateSubscriptionRequest) (*subflowv1.Subscription, error) {
	p, err := s.PlanRepo.Get(ctx, req.PlanId)
	if errors.Is(err, store.ErrPlanNotFound) {
		return nil, status.Error(codes.NotFound, "plan not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get plan: %v", err)
	}

	now := time.Now().UTC()
	subID := uuid.NewString()
	wfInput := subscription.SubscriptionInput{
		SubscriptionID:  subID,
		UserID:          req.UserId,
		PlanID:          p.ID,
		PlanCode:        p.Code,
		BillingInterval: p.BillingInterval,
		IntegrationHost: integrationFor(p, s.DefaultIntegration),
		PriceCents:      p.PriceCents,
		PeriodStart:     now,
		PeriodEnd:       now.Add(p.BillingInterval),
		Context:         subscription.Context(req.InitialContext),
	}

	_, err = s.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "subscription:" + subID,
		TaskQueue: s.TaskQueue,
	}, workflow.SubscriptionWorkflow, wfInput)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start workflow: %v", err)
	}

	// Optimistic projection so List works immediately (worker will overwrite as it progresses).
	_ = s.Projection.Upsert(ctx, store.SubscriptionView{
		ID:           subID,
		UserID:       req.UserId,
		PlanID:       p.ID,
		Phase:        activity.PhasePending,
		PeriodStart:  wfInput.PeriodStart,
		PeriodEnd:    wfInput.PeriodEnd,
		RenewalCount: 0,
		Context:      wfInput.Context,
	})

	return &subflowv1.Subscription{
		Id:           subID,
		UserId:       req.UserId,
		PlanId:       p.ID,
		Phase:        activity.PhasePending,
		PeriodStart:  timestamppb.New(wfInput.PeriodStart),
		PeriodEnd:    timestamppb.New(wfInput.PeriodEnd),
		RenewalCount: 0,
		Context:      map[string]string(wfInput.Context),
	}, nil
}

func (s *SubscriptionService) CancelSubscription(ctx context.Context, req *subflowv1.CancelSubscriptionRequest) (*subflowv1.CancelSubscriptionResponse, error) {
	err := s.Temporal.SignalWorkflow(ctx, "subscription:"+req.Id, "", workflow.SignalCancelSubscription, nil)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "signal workflow: %v", err)
	}
	return &subflowv1.CancelSubscriptionResponse{}, nil
}

func (s *SubscriptionService) GetSubscription(ctx context.Context, req *subflowv1.GetSubscriptionRequest) (*subflowv1.Subscription, error) {
	// Try query first (live state from Temporal).
	res, err := s.Temporal.QueryWorkflow(ctx, "subscription:"+req.Id, "", workflow.QuerySubscriptionStatus)
	if err == nil {
		var st workflow.SubscriptionStatus
		if err := res.Get(&st); err == nil {
			view, _ := s.Projection.Get(ctx, req.Id)
			return statusToProto(req.Id, view, st), nil
		}
	}

	// Fall back to projection (workflow may have completed).
	view, err := s.Projection.Get(ctx, req.Id)
	if errors.Is(err, store.ErrSubscriptionNotFound) {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get projection: %v", err)
	}
	return viewToProto(view), nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, req *subflowv1.ListSubscriptionsRequest) (*subflowv1.ListSubscriptionsResponse, error) {
	views, err := s.Projection.List(ctx, req.UserId, req.Phase)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	out := &subflowv1.ListSubscriptionsResponse{Subscriptions: make([]*subflowv1.Subscription, 0, len(views))}
	for _, v := range views {
		out.Subscriptions = append(out.Subscriptions, viewToProto(v))
	}
	return out, nil
}

func integrationFor(p plan.Plan, fallback string) string {
	if p.IntegrationEndpoint != "" {
		return p.IntegrationEndpoint
	}
	return fallback
}

func statusToProto(id string, view store.SubscriptionView, st workflow.SubscriptionStatus) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:              id,
		UserId:          view.UserID,
		PlanId:          view.PlanID,
		Phase:           st.Phase,
		PeriodStart:     timestamppb.New(st.PeriodStart),
		PeriodEnd:       timestamppb.New(st.PeriodEnd),
		RenewalCount:    int32(st.RenewalCount),
		Context:         map[string]string(st.Context),
		CancelRequested: st.CancelRequested,
	}
}

func viewToProto(v store.SubscriptionView) *subflowv1.Subscription {
	return &subflowv1.Subscription{
		Id:              v.ID,
		UserId:          v.UserID,
		PlanId:          v.PlanID,
		Phase:           v.Phase,
		PeriodStart:     timestamppb.New(v.PeriodStart),
		PeriodEnd:       timestamppb.New(v.PeriodEnd),
		RenewalCount:    int32(v.RenewalCount),
		Context:         map[string]string(v.Context),
		CancelRequested: v.CancelRequested,
	}
}
```

- [ ] **Step 2: Verify**

Run:
```bash
go build ./internal/server/...
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/server/subscriptions.go
git commit -m "feat(server): subscription handlers translating RPCs to Temporal client calls"
```

---

## Task 17: `cmd/api` binary

**Files:**
- Create: `cmd/api/main.go`

- [ ] **Step 1: Create `cmd/api/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"github.com/martavoi/subflow/internal/config"
	"github.com/martavoi/subflow/internal/server"
	"github.com/martavoi/subflow/internal/store"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
)

// AggregateService composes plan + subscription services into a single
// SubflowService gRPC implementation.
type AggregateService struct {
	subflowv1.UnimplementedSubflowServiceServer
	*server.PlanService
	*server.SubscriptionService
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadAPI()
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, db, err := store.Connect(mongoCtx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		logger.Error("mongo connect", "err", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(context.Background())

	planRepo := store.NewPlanRepository(db)
	projection := store.NewSubscriptionProjectionRepository(db)

	if err := planRepo.EnsureIndexes(ctx); err != nil {
		logger.Error("plan indexes", "err", err)
		os.Exit(1)
	}
	if err := projection.EnsureIndexes(ctx); err != nil {
		logger.Error("projection indexes", "err", err)
		os.Exit(1)
	}

	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		logger.Error("temporal dial", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	svc := &AggregateService{
		PlanService: &server.PlanService{Repo: planRepo},
		SubscriptionService: &server.SubscriptionService{
			Temporal:           tc,
			TaskQueue:          "subflow",
			PlanRepo:           planRepo,
			Projection:         projection,
			DefaultIntegration: cfg.IntegrationHost,
		},
	}

	g := grpc.NewServer()
	subflowv1.RegisterSubflowServiceServer(g, svc)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}
	logger.Info("subflow-api listening", "port", cfg.GRPCPort)

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		g.GracefulStop()
	}()

	if err := g.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Build**

Run:
```bash
go build -o bin/api ./cmd/api
```
Expected: `bin/api` produced.

- [ ] **Step 3: Commit**

```bash
git add cmd/api/main.go
git commit -m "feat(cmd/api): gRPC server bootstrap (Mongo + Temporal client)"
```

---

## Task 18: `cmd/worker` binary

**Files:**
- Create: `cmd/worker/main.go`

- [ ] **Step 1: Create `cmd/worker/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/config"
	"github.com/martavoi/subflow/internal/eventbus"
	"github.com/martavoi/subflow/internal/integration"
	"github.com/martavoi/subflow/internal/store"
	wfpkg "github.com/martavoi/subflow/internal/workflow"
	tactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.LoadWorker()
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, db, err := store.Connect(mongoCtx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		logger.Error("mongo connect", "err", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(context.Background())

	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		logger.Error("temporal dial", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	publisher := eventbus.NewStdoutPublisher(logger)
	intClient := integration.NewClient()
	defer intClient.Close()

	paymentActs := &activity.PaymentActivities{
		TransientFailureRate: cfg.PaymentTransientRate,
		TerminalFailureRate:  cfg.PaymentTerminalRate,
	}
	eventActs := &activity.EventActivities{Publisher: publisher}
	intActs := &activity.IntegrationActivities{Client: intClient}
	projectionActs := &activity.ProjectionActivities{
		Repo: store.NewSubscriptionProjectionRepository(db),
	}

	w := worker.New(tc, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(wfpkg.SubscriptionWorkflow)
	w.RegisterActivityWithOptions(paymentActs.ChargePayment, tactivity.RegisterOptions{Name: "ChargePayment"})
	w.RegisterActivityWithOptions(eventActs.PublishSubscriptionEvent, tactivity.RegisterOptions{Name: "PublishSubscriptionEvent"})
	w.RegisterActivityWithOptions(intActs.NotifyIntegrationService, tactivity.RegisterOptions{Name: "NotifyIntegrationService"})
	w.RegisterActivityWithOptions(projectionActs.UpdateSubscriptionProjection, tactivity.RegisterOptions{Name: "UpdateSubscriptionProjection"})

	logger.Info("subflow-worker starting", "task_queue", cfg.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		logger.Error("worker run", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Build**

Run:
```bash
go build -o bin/worker ./cmd/worker
```
Expected: `bin/worker` produced.

- [ ] **Step 3: Run all tests one more time**

Run:
```bash
go test ./...
```
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/worker/main.go
git commit -m "feat(cmd/worker): Temporal worker registers SubscriptionWorkflow + activities"
```

---

## Task 19: Three Dockerfiles + compose.yml

**Files:**
- Create: `cmd/api/Dockerfile`
- Create: `cmd/worker/Dockerfile`
- Create: `cmd/mock-integration/Dockerfile`
- Create: `compose.yml`

- [ ] **Step 1: Create `cmd/api/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api /api
ENTRYPOINT ["/api"]
```

- [ ] **Step 2: Create `cmd/worker/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /worker
ENTRYPOINT ["/worker"]
```

- [ ] **Step 3: Create `cmd/mock-integration/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mock ./cmd/mock-integration

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mock /mock
ENTRYPOINT ["/mock"]
```

- [ ] **Step 4: Create `compose.yml`**

```yaml
services:
  mongo:
    image: mongo:7
    ports:
      - "27017:27017"
    volumes:
      - mongodata:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"]
      interval: 5s
      timeout: 5s
      retries: 12

  temporal:
    image: temporalio/temporal:latest
    command: ["server", "start-dev", "--ip", "0.0.0.0", "--ui-ip", "0.0.0.0", "--db-filename", "/data/temporal.db"]
    ports:
      - "7233:7233"     # frontend gRPC
      - "8233:8233"     # web UI
    volumes:
      - temporaldata:/data
    healthcheck:
      test: ["CMD", "/usr/local/bin/temporal", "operator", "cluster", "health", "--address", "localhost:7233"]
      interval: 5s
      timeout: 5s
      retries: 12

  mock-integration:
    build:
      context: .
      dockerfile: cmd/mock-integration/Dockerfile
    ports:
      - "50052:50052"
    environment:
      MOCK_GRPC_PORT: "50052"
      FAILURE_RATE: "${FAILURE_RATE:-0.3}"
      LATENCY_MS: "${LATENCY_MS:-100}"
      TERMINAL_FAILURE_RATE: "${TERMINAL_FAILURE_RATE:-0.0}"

  subflow-api:
    build:
      context: .
      dockerfile: cmd/api/Dockerfile
    ports:
      - "50051:50051"
    depends_on:
      mongo: { condition: service_healthy }
      temporal: { condition: service_healthy }
    environment:
      API_GRPC_PORT: "50051"
      TEMPORAL_HOST: "temporal:7233"
      TEMPORAL_NAMESPACE: "default"
      MONGO_URI: "mongodb://mongo:27017"
      MONGO_DATABASE: "subflow"
      INTEGRATION_HOST: "mock-integration:50052"

  subflow-worker:
    build:
      context: .
      dockerfile: cmd/worker/Dockerfile
    depends_on:
      mongo: { condition: service_healthy }
      temporal: { condition: service_healthy }
    environment:
      TEMPORAL_HOST: "temporal:7233"
      TEMPORAL_NAMESPACE: "default"
      TASK_QUEUE: "subflow"
      MONGO_URI: "mongodb://mongo:27017"
      MONGO_DATABASE: "subflow"
      PAYMENT_TRANSIENT_RATE: "0.0"
      PAYMENT_TERMINAL_RATE: "0.0"

volumes:
  mongodata:
  temporaldata:
```

- [ ] **Step 5: Bring up the stack**

Run:
```bash
podman compose up -d --build
podman compose ps
```
Expected: 5 services running. `subflow-api` healthy on `:50051`, `temporal` UI reachable at `http://localhost:8233`, `mock-integration` on `:50052`.

If you don't have `podman compose`, use `docker compose` — same commands.

- [ ] **Step 6: Smoke test from host**

Run (requires `grpcurl`):
```bash
grpcurl -plaintext localhost:50051 list
```
Expected: lists `subflow.v1.SubflowService`.

- [ ] **Step 7: Tear down**

Run:
```bash
podman compose down
```

- [ ] **Step 8: Commit**

```bash
git add cmd/*/Dockerfile compose.yml
git commit -m "feat(deploy): three Dockerfiles + 5-service compose stack (mongo, temporal-dev, api, worker, mock)"
```

---

## Task 20: Taskfile commands

**Files:**
- Create: `Taskfile.yml`

- [ ] **Step 1: Create `Taskfile.yml`**

```yaml
version: '3'

vars:
  API_HOST: localhost:50051
  PROTO_DIR: api/v1

env:
  CGO_ENABLED: "0"

tasks:
  proto-gen:
    desc: Regenerate protobuf code
    cmds:
      - buf generate

  build:
    desc: Build all binaries to ./bin
    cmds:
      - go build -o bin/api ./cmd/api
      - go build -o bin/worker ./cmd/worker
      - go build -o bin/mock-integration ./cmd/mock-integration

  test:
    desc: Run all tests
    cmds:
      - go test ./... -v

  up:
    desc: Bring up the stack (podman compose)
    cmds:
      - podman compose up -d --build

  down:
    desc: Stop and remove containers (preserves volumes)
    cmds:
      - podman compose down

  reset:
    desc: Stop and wipe volumes (Mongo data + Temporal SQLite)
    cmds:
      - podman compose down -v

  logs:
    desc: Tail logs of all services
    cmds:
      - podman compose logs -f

  ui:
    desc: Open the Temporal Web UI
    cmds:
      - open http://localhost:8233

  list-interfaces:
    desc: List exposed gRPC services
    cmds:
      - grpcurl -plaintext {{.API_HOST}} list

  seed-plan:
    desc: 'Create a sample 30-second-period plan named "demo-fast" pointing at mock-integration'
    cmds:
      - |
        grpcurl -plaintext -d '{
          "code": "demo-fast",
          "name": "Demo Fast (30s renewal)",
          "billing_interval": "30s",
          "price_cents": 999,
          "integration_endpoint": "mock-integration:50052"
        }' {{.API_HOST}} subflow.v1.SubflowService/CreatePlan

  list-plans:
    desc: List all plans
    cmds:
      - grpcurl -plaintext {{.API_HOST}} subflow.v1.SubflowService/ListPlans

  create-subscription:
    desc: 'Create a subscription. Vars: USER, PLAN_ID'
    cmds:
      - |
        grpcurl -plaintext -d '{
          "user_id": "{{.USER}}",
          "plan_id": "{{.PLAN_ID}}",
          "initial_context": {"card_id":"card_001"}
        }' {{.API_HOST}} subflow.v1.SubflowService/CreateSubscription
    requires:
      vars: [USER, PLAN_ID]

  cancel-subscription:
    desc: 'Cancel a subscription. Var: ID'
    cmds:
      - |
        grpcurl -plaintext -d '{"id":"{{.ID}}"}' \
          {{.API_HOST}} subflow.v1.SubflowService/CancelSubscription
    requires:
      vars: [ID]

  get-subscription:
    desc: 'Get subscription state. Var: ID'
    cmds:
      - |
        grpcurl -plaintext -d '{"id":"{{.ID}}"}' \
          {{.API_HOST}} subflow.v1.SubflowService/GetSubscription
    requires:
      vars: [ID]

  list-subscriptions:
    desc: 'List subscriptions. Optional vars: USER, PHASE'
    cmds:
      - |
        grpcurl -plaintext -d '{"user_id":"{{.USER}}","phase":"{{.PHASE}}"}' \
          {{.API_HOST}} subflow.v1.SubflowService/ListSubscriptions

  break-integration:
    desc: Stop mock-integration to demonstrate Temporal retry behavior
    cmds:
      - podman compose stop mock-integration

  fix-integration:
    desc: Restart mock-integration so retried activities drain
    cmds:
      - podman compose start mock-integration
```

- [ ] **Step 2: Smoke-test a couple of tasks**

Run:
```bash
task --list
```
Expected: prints all tasks above.

(Don't run `task up` yet unless the stack is rebuilt; subsequent README walkthrough handles that.)

- [ ] **Step 3: Commit**

```bash
git add Taskfile.yml
git commit -m "feat(taskfile): build/test/up/seed/cancel/list/break-integration commands"
```

---

## Task 21: README + cheatsheet

**Files:**
- Create: `README.md`
- Create: `docs/architecture.md`
- Create: `docs/temporal-cheatsheet.md`

- [ ] **Step 1: Write `README.md`**

```markdown
# subflow

A minimal, open-source-friendly Go playground that models a subscription lifecycle on top of [Temporal](https://temporal.io). It demonstrates how durable workflows replace a polling renewal scheduler, signals replace cancel APIs, and activities with retry policies replace ad-hoc retry loops.

> **Status:** Learning POC. Not production-ready. Deliberately minimal.

## Quickstart

```bash
git clone https://github.com/martavoi/subflow
cd subflow
task up                       # podman compose up -d --build
open http://localhost:8233    # Temporal Web UI

task seed-plan                # creates a 30s-period demo plan
task list-plans               # find the plan ID

task create-subscription USER=alice PLAN_ID=<plan-id>
task list-subscriptions

# Watch the renewals stream by in the Temporal Web UI: each billing period is
# a discrete workflow run, chained via Continue-As-New.

task cancel-subscription ID=<sub-id>
# Subscription remains active until the current period ends, then deactivates.
```

## What you'll see in the Web UI

1. **A workflow per subscription**, ID-prefixed `subscription:`.
2. **Chained Continue-As-New runs**, one per billing period — bounded history regardless of subscription duration.
3. **Activity retries with backoff** when you `task break-integration` (mock-integration is unavailable). Restart with `task fix-integration` and watch the queued retries drain.
4. **Cancel-as-signal** semantics: signal arrives mid-period, workflow honors end-of-period, then runs deactivation.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the diagram and component breakdown. Cheat-sheet of subscription concepts mapped to Temporal primitives is in [docs/temporal-cheatsheet.md](docs/temporal-cheatsheet.md).

## Stack

- Go 1.23
- Temporal Go SDK + Temporal dev server (SQLite-backed, embedded Web UI)
- Mongo 7 for plans + subscription read-model
- gRPC + buf
- Podman / Docker compose

## Failure injection

`mock-integration` honors three env vars (set via compose.yml or `.env`):

| Var | Effect |
|---|---|
| `FAILURE_RATE` | Probability of returning gRPC `Unavailable` (retryable) |
| `TERMINAL_FAILURE_RATE` | Probability of returning gRPC `FailedPrecondition` (non-retryable; will fail the workflow) |
| `LATENCY_MS` | Artificial latency per call |

The worker also has `PAYMENT_TRANSIENT_RATE` and `PAYMENT_TERMINAL_RATE` for the `ChargePayment` activity.

## Roadmap (out of POC scope)

- Workflow versioning helpers (`workflow.GetVersion`)
- Plan upgrades/downgrades mid-period via signals
- Pause / resume signals
- Replace stdout event publisher with Kafka
- Production-grade Temporal deployment (Cassandra, multi-region)

## License

MIT — see [LICENSE](LICENSE).
```

- [ ] **Step 2: Write `docs/architecture.md`**

```markdown
# Architecture

```
┌─────────────────┐    gRPC    ┌──────────────────────┐
│  client (CLI/   │──────────▶│  subflow-api         │
│  grpcurl)       │            │  (gRPC :50051,       │
└─────────────────┘            │   Temporal client)   │
                               └──────────┬───────────┘
                                          │ StartWorkflow / SignalWorkflow /
                                          │ QueryWorkflow + Mongo CRUD
                                          ▼
                               ┌──────────────────────┐         ┌─────────────────────┐
                               │  Temporal dev server │◀────────│  Web UI :8233       │
                               │  (SQLite-backed,     │         │  (built into the    │
                               │   embedded UI)       │         │   dev-server image) │
                               └──────────┬───────────┘         └─────────────────────┘
                                          │ task queue: "subflow"
                                          ▼
                               ┌──────────────────────┐
                               │  subflow-worker      │
                               │  - SubscriptionWF    │
                               │  - Activities        │
                               └──────────┬───────────┘
                                          │ activity calls (gRPC) + Mongo writes
                                          ▼
                               ┌──────────────────────┐
                               │  mock-integration    │
                               │  (gRPC :50052)       │
                               └──────────────────────┘

┌──────────────────────┐
│  Mongo               │
│  - plans             │
│  - subscriptions_view│
└──────────────────────┘
```

## Components

| Component | Role |
|---|---|
| `subflow-api` | gRPC server on :50051. Translates RPCs into Temporal client calls and Mongo CRUD. No business logic. |
| `subflow-worker` | Temporal worker. Hosts `SubscriptionWorkflow` and the four activities. Polls the `subflow` task queue. |
| `mock-integration` | gRPC server on :50052 implementing `IntegrationService`. Configurable failure/latency knobs. |
| `temporal` | Single-binary dev server. SQLite persistence. Bundled Web UI on :8233. |
| `mongo` | Mongo 7. Holds plans collection (source of truth) and subscriptions_view collection (read-model). |

## Why this shape?

- **Workflow as state machine**: each subscription is *one workflow execution* (per period, via Continue-As-New). The workflow carries its own state (period, context, cancel flag) — no polling scheduler scanning a database.
- **Activities for I/O**: payment, event publishing, integration callouts, projection writes — each with its own retry policy named for the failure mode it handles.
- **Read-model projection**: Temporal is the source of truth for live state; Mongo's `subscriptions_view` exists only because Temporal Visibility isn't the right tool for ad-hoc listing.
- **Mock integration**: a tiny gRPC service with failure-injection knobs makes Temporal's retry semantics tangible — stop the container, watch backoffs in the Web UI.
```

- [ ] **Step 3: Write `docs/temporal-cheatsheet.md`**

```markdown
# subflow ↔ Temporal cheat sheet

| Subscription concept | Temporal primitive | Where in code |
|---|---|---|
| Subscription instance | Workflow execution (ID `subscription:<id>`) | `internal/workflow/subscription.go` |
| Renewal cadence | `workflow.NewTimer` durable timer | `AwaitPeriodEndOrCancellation` in `internal/workflow/lifecycle.go` |
| End of subscription period | Continue-As-New | `ContinueIntoNextPeriod` |
| Cancel request | Signal `subscription.cancel` | `SignalCancelSubscription` constant |
| Status read | Query `subscription.status` | `QuerySubscriptionStatus` constant + `SubscriptionState.AsStatus` |
| Charge / publish / integration call | Activities with named retry policy | `internal/activity/*.go` |
| Mutable subscription context | Workflow state (carried in `SubscriptionInput.Context`) | `internal/domain/subscription/context.go` |
| Read-model for listing | Mongo `subscriptions_view` updated by `UpdateSubscriptionProjection` activity | `internal/store/projection.go` |
| Idempotency token | `<workflowID>:<runID>:<suffix>` | `activityRef` in `internal/workflow/lifecycle.go` |

## Things to play with in the Web UI (http://localhost:8233)

1. **Find a running subscription workflow.** It's `subscription:<uuid>`. Open it.
2. **Look at "Pending Activities"** — when mock-integration is down, this fills with retries.
3. **Click "Continue As New"** events in the history — each renewal is one of these.
4. **Send a signal from the UI**: Workflow → Send Signal → `subscription.cancel`. Watch the timer fire at period end and deactivation activities run.
5. **Run a query from the UI**: Workflow → Query → `subscription.status`. Returns the live in-memory state.

## Why Continue-As-New per renewal (not every N renewals)?

Each billing period is its own discrete run with bounded history (~20 events). The workflow ID never changes; signals and queries continue to address the latest run automatically. This keeps history footprint identical for a 1-year and a 50-year subscription, and makes each period visible as one row in the UI — the renewal history *is* the workflow history.
```

- [ ] **Step 4: Commit**

```bash
git add README.md docs/architecture.md docs/temporal-cheatsheet.md
git commit -m "docs: README quickstart + architecture + Temporal cheatsheet"
```

---

## Task 22: Manual end-to-end validation

**Files:** none (operational walkthrough)

- [ ] **Step 1: Bring up the stack with default knobs**

Run:
```bash
task up
```
Wait until all services healthy (`podman compose ps`).

- [ ] **Step 2: Open the Web UI**

```bash
task ui
```
Expected: Temporal Web UI loads.

- [ ] **Step 3: Seed a fast plan**

```bash
task seed-plan
```
Expected: response contains a plan with `code: demo-fast` and an `id`.

```bash
task list-plans
```
Note the plan ID for the next step.

- [ ] **Step 4: Create a subscription**

```bash
task create-subscription USER=alice PLAN_ID=<plan-id-from-step-3>
```
Expected: response contains `id` (subscription ID), `phase: pending`.

In the Temporal UI: a workflow `subscription:<id>` should appear and quickly transition through activation activities.

- [ ] **Step 5: Watch a renewal happen**

Wait ~30 seconds (the `demo-fast` billing interval).

In the Web UI: the workflow should show a Continue-As-New event and the new run should run renewal activities.

In the worker logs (`task logs`): you should see `subflow.event` JSON lines with `"type":"subscription.renew"`.

- [ ] **Step 6: Demonstrate retry on integration failure**

```bash
task break-integration
```
Expected: mock-integration container stopped.

Wait through the next renewal. In the Web UI: the workflow's `NotifyIntegrationService` activity should show repeated retry attempts.

```bash
task fix-integration
```
Expected: container restarts; queued retries drain.

- [ ] **Step 7: Cancel the subscription**

```bash
task get-subscription ID=<sub-id>          # check current period_end
task cancel-subscription ID=<sub-id>
```

In the Web UI: signal arrives, `cancel_requested` becomes true (visible via query). At the next period end the workflow runs deactivation activities and completes.

```bash
task list-subscriptions PHASE=deactivated
```
Expected: cancelled sub appears with `phase: deactivated`.

- [ ] **Step 8: Tear down (preserving volumes)**

```bash
task down
```

To wipe state for a fresh run:
```bash
task reset
```

- [ ] **Step 9: Final commit (README walkthrough section if anything was unclear)**

If the walkthrough revealed any rough edges (missing task, unclear log message, broken healthcheck), fix and commit:
```bash
git add -A
git commit -m "chore: smooth e2e walkthrough rough edges"
```

If nothing needed fixing, no commit.

---

## Self-Review

**1. Spec coverage check:**

| Spec section | Implementing tasks |
|---|---|
| §1 Background and goals | T2 (proto contract) + T3 (domain) — establishes the subscription model |
| §2 Architecture (3 binaries, 5-service stack) | T17 (api), T18 (worker), T4 (mock-integration), T19 (compose) |
| §3 Workflow design (DDD lifecycle verbs, CAN-per-renewal, signals/queries, retry policies, idempotency) | T11 (signals/state) + T12 (lifecycle verbs) + T13 (entry + tests) + T6 (retry policies) + activityRef in T12 |
| §4 gRPC API | T2 (proto) + T15 (plan handlers) + T16 (subscription handlers) |
| §5 Persistence (Mongo collections, indexes, projection) | T5 (store) + T15/T16 (handlers using store) + T10 (projection activity) |
| §6 Failure scenarios | T6 (retry policies define semantics) + T22 step 6 (manual demo) |
| §7 Repository layout | All file-creation tasks together |
| §8 Testing approach | T3 (period_test) + T13 (workflow tests) — pure domain + workflow lifecycle |
| §9 Developer workflow | T20 (Taskfile) + T21 (README) + T22 (manual e2e) |
| §10 Out of scope | T21 (README Roadmap section explicitly lists exclusions) |

All sections covered.

**2. Placeholder scan:** No `TBD`, `TODO`, or "implement later". Every code block is complete. The one place that initially looked sketchy — the test file's import collision — is resolved with the `activityPkg` aliased import shown in the corrected version.

**3. Type consistency:**
- `SubscriptionInput` fields used in T12 (`activityRef`, `chargeAndPublish`, …) match definitions in T3.
- Activity input/result types referenced in T12 (`ChargePaymentInput`/`Result`, `PublishEventInput`, `IntegrationCallInput`/`Result`, `ProjectionUpdate`) match definitions in T7/T8/T9/T10 exactly.
- Signal/query name constants (`SignalCancelSubscription`, `QuerySubscriptionStatus`) defined once in T11, used in T13 (workflow entry), T16 (subscription handler), T13 (test).
- Phase constants (`PhasePending`, `PhaseActive`, `PhaseCancelling`, `PhaseDeactivated`) defined once in T10, used in T12 and T16.
- Activity registration names (`"ChargePayment"`, `"PublishSubscriptionEvent"`, `"NotifyIntegrationService"`, `"UpdateSubscriptionProjection"`) match between worker registration (T18), workflow `ExecuteActivity` calls (T12), and test mock registration (T13).

All consistent.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-09-subflow-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
