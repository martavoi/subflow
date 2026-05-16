package integration

import (
	"context"
	"fmt"
	"sync"

	subflowv1 "github.com/martavoi/subflow/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client maintains lazy-dialed gRPC connections to integration endpoints
// (one connection per host). Exposes a SubscriptionHooks stub via Hooks.
type Client struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewClient() *Client {
	return &Client{conns: make(map[string]*grpc.ClientConn)}
}

func (c *Client) Hooks(endpoint string) (subflowv1.SubscriptionHooksClient, error) {
	conn, err := c.connect(endpoint)
	if err != nil {
		return nil, err
	}
	return subflowv1.NewSubscriptionHooksClient(conn), nil
}

func (c *Client) connect(endpoint string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[endpoint]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %q: %w", endpoint, err)
	}
	c.conns[endpoint] = conn
	return conn, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Dispatch calls the single Dispatch rpc on the integration service.
func (c *Client) Dispatch(ctx context.Context, endpoint string, ev *subflowv1.Event) error {
	stub, err := c.Hooks(endpoint)
	if err != nil {
		return err
	}
	_, err = stub.Dispatch(ctx, ev)
	return err
}
