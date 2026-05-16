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
	subflowv1.UnimplementedSubscriptionHooksServer

	failureRate         float64
	terminalFailureRate float64
	latency             time.Duration
	logger              *slog.Logger

	mu      sync.Mutex
	seenRef map[string]bool
}

func (s *server) maybeFail(hookType, id string) error {
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	s.mu.Lock()
	if s.seenRef[id] {
		s.mu.Unlock()
		s.logger.Info("idempotent replay", "hook", hookType, "id", id)
		return nil
	}
	s.mu.Unlock()

	r := rand.Float64()
	if r < s.terminalFailureRate {
		s.logger.Warn("injected terminal", "hook", hookType, "id", id)
		return status.Error(codes.FailedPrecondition, "injected terminal failure")
	}
	if r < s.terminalFailureRate+s.failureRate {
		s.logger.Warn("injected transient", "hook", hookType, "id", id)
		return status.Error(codes.Unavailable, "injected transient failure")
	}

	s.mu.Lock()
	s.seenRef[id] = true
	s.mu.Unlock()
	s.logger.Info("hook handled", "hook", hookType, "id", id)
	return nil
}

func (s *server) Dispatch(_ context.Context, ev *subflowv1.Event) (*subflowv1.HookAck, error) {
	s.logger.Info("dispatch", "type", ev.Type, "subscription_id", ev.SubscriptionId)
	if err := s.maybeFail(ev.Type, ev.Id); err != nil {
		return nil, err
	}
	return &subflowv1.HookAck{}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := getenv("MOCK_GRPC_PORT", "50052")
	failureRate := mustFloat(getenv("FAILURE_RATE", "0.0"))
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
		seenRef:             make(map[string]bool),
	}

	g := grpc.NewServer()
	subflowv1.RegisterSubscriptionHooksServer(g, s)

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
