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
