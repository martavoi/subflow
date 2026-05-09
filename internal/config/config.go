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
	TaskQueue         string // must match the worker's TASK_QUEUE
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
		TaskQueue:         getenv("TASK_QUEUE", "subflow"),
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
