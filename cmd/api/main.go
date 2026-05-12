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
	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/config"
	"github.com/martavoi/subflow/internal/server"
	"github.com/martavoi/subflow/internal/store"
	subflowtemporal "github.com/martavoi/subflow/internal/temporal"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// AggregateService composes plan + subscription + billing services into the
// single SubflowService gRPC implementation.
type AggregateService struct {
	*server.PlanService
	*server.SubscriptionService
	*server.BillingEventsService
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

	// Mongo
	mongoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mongoClient, db, err := store.Connect(mongoCtx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		logger.Error("mongo connect", "err", err)
		os.Exit(1)
	}
	defer mongoClient.Disconnect(context.Background())

	planRepo := store.NewPlanRepository(db)
	if err := planRepo.EnsureIndexes(ctx); err != nil {
		logger.Error("plan indexes", "err", err)
		os.Exit(1)
	}

	billingStore := billing.NewMongoStore(db)
	if err := billingStore.EnsureIndexes(ctx); err != nil {
		logger.Error("billing indexes", "err", err)
		os.Exit(1)
	}

	// Temporal
	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		logger.Error("temporal dial", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	// Register custom search attributes (idempotent).
	saCtx, saCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := subflowtemporal.EnsureSearchAttributes(saCtx, tc, cfg.TemporalNamespace, logger); err != nil {
		saCancel()
		logger.Error("ensure search attributes", "err", err)
		os.Exit(1)
	}
	saCancel()

	// gRPC server
	svc := &AggregateService{
		PlanService: &server.PlanService{Repo: planRepo},
		SubscriptionService: &server.SubscriptionService{
			Temporal:  tc,
			Namespace: cfg.TemporalNamespace,
			TaskQueue: cfg.TaskQueue,
			PlanRepo:  planRepo,
		},
		BillingEventsService: &server.BillingEventsService{Events: billingStore},
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
