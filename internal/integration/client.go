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
