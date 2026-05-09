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
