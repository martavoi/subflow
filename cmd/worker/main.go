package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martavoi/subflow/internal/activity"
	"github.com/martavoi/subflow/internal/billing"
	"github.com/martavoi/subflow/internal/config"
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

	billingStore := billing.NewMongoStore(db)
	if err := billingStore.EnsureIndexes(ctx); err != nil {
		logger.Error("billing indexes", "err", err)
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

	intClient := integration.NewClient()
	defer intClient.Close()

	paymentActs := &activity.PaymentActivities{
		TransientFailureRate: cfg.PaymentTransientRate,
		TerminalFailureRate:  cfg.PaymentTerminalRate,
	}
	billingActs := &activity.BillingActivities{Events: billingStore}
	hookActs := &activity.HookActivities{Client: intClient}

	w := worker.New(tc, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(wfpkg.SubscriptionWorkflow)

	// Core activities
	w.RegisterActivityWithOptions(paymentActs.ChargePayment, tactivity.RegisterOptions{Name: "ChargePayment"})
	w.RegisterActivityWithOptions(billingActs.RecordBillingEvent, tactivity.RegisterOptions{Name: "RecordBillingEvent"})

	// 10 hook activities
	w.RegisterActivityWithOptions(hookActs.OnTrialStarted, tactivity.RegisterOptions{Name: "OnTrialStarted"})
	w.RegisterActivityWithOptions(hookActs.OnTrialWillEnd, tactivity.RegisterOptions{Name: "OnTrialWillEnd"})
	w.RegisterActivityWithOptions(hookActs.OnActivated, tactivity.RegisterOptions{Name: "OnActivated"})
	w.RegisterActivityWithOptions(hookActs.OnRenewed, tactivity.RegisterOptions{Name: "OnRenewed"})
	w.RegisterActivityWithOptions(hookActs.OnPastDue, tactivity.RegisterOptions{Name: "OnPastDue"})
	w.RegisterActivityWithOptions(hookActs.OnRecovered, tactivity.RegisterOptions{Name: "OnRecovered"})
	w.RegisterActivityWithOptions(hookActs.OnCanceled, tactivity.RegisterOptions{Name: "OnCanceled"})
	w.RegisterActivityWithOptions(hookActs.OnDeactivated, tactivity.RegisterOptions{Name: "OnDeactivated"})
	w.RegisterActivityWithOptions(hookActs.OnPaymentSucceeded, tactivity.RegisterOptions{Name: "OnPaymentSucceeded"})
	w.RegisterActivityWithOptions(hookActs.OnPaymentFailed, tactivity.RegisterOptions{Name: "OnPaymentFailed"})

	logger.Info("subflow-worker starting", "task_queue", cfg.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		logger.Error("worker run", "err", err)
		os.Exit(1)
	}
}
