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

// DispatchLifecycle calls one of the lifecycle hook methods on the integration
// service. Centralizes the dispatch so the activity layer doesn't need a switch.
func (c *Client) DispatchLifecycle(ctx context.Context, endpoint, hookName string, ev *subflowv1.LifecycleEvent) error {
	stub, err := c.Hooks(endpoint)
	if err != nil {
		return err
	}
	switch hookName {
	case "subscription.trial_started":
		_, err = stub.OnTrialStarted(ctx, ev)
	case "subscription.trial_will_end":
		_, err = stub.OnTrialWillEnd(ctx, ev)
	case "subscription.activated":
		_, err = stub.OnActivated(ctx, ev)
	case "subscription.renewed":
		_, err = stub.OnRenewed(ctx, ev)
	case "subscription.past_due":
		_, err = stub.OnPastDue(ctx, ev)
	case "subscription.recovered":
		_, err = stub.OnRecovered(ctx, ev)
	case "subscription.canceled":
		_, err = stub.OnCanceled(ctx, ev)
	case "subscription.deactivated":
		_, err = stub.OnDeactivated(ctx, ev)
	default:
		return fmt.Errorf("unknown lifecycle hook: %s", hookName)
	}
	return err
}

// DispatchPayment calls one of the payment hook methods.
func (c *Client) DispatchPayment(ctx context.Context, endpoint, hookName string, ev *subflowv1.PaymentEvent) error {
	stub, err := c.Hooks(endpoint)
	if err != nil {
		return err
	}
	switch hookName {
	case "payment.succeeded":
		_, err = stub.OnPaymentSucceeded(ctx, ev)
	case "payment.failed":
		_, err = stub.OnPaymentFailed(ctx, ev)
	default:
		return fmt.Errorf("unknown payment hook: %s", hookName)
	}
	return err
}
