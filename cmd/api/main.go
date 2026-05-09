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
	"google.golang.org/grpc/reflection"
)

// AggregateService composes plan + subscription services into a single
// SubflowService gRPC implementation.
type AggregateService struct {
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
	reflection.Register(g)

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
