package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
)

// Custom search attribute names used by SubscriptionWorkflow.
const (
	AttrUserId    = "SubflowUserId"
	AttrPlanCode  = "SubflowPlanCode"
	AttrPhase     = "SubflowPhase"
	AttrPeriodEnd = "SubflowPeriodEnd"
	AttrTrialEnd  = "SubflowTrialEnd"
)

var attrTypes = []struct {
	Name string
	Type enumspb.IndexedValueType
}{
	{AttrUserId, enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	{AttrPlanCode, enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	{AttrPhase, enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	{AttrPeriodEnd, enumspb.INDEXED_VALUE_TYPE_DATETIME},
	{AttrTrialEnd, enumspb.INDEXED_VALUE_TYPE_DATETIME},
}

// EnsureSearchAttributes registers the custom subflow search attributes on
// the Temporal cluster. Already-existing attributes are treated as success.
// Run this once at API startup; safe to re-run.
func EnsureSearchAttributes(ctx context.Context, c client.Client, namespace string, logger *slog.Logger) error {
	op := c.OperatorService()
	for _, a := range attrTypes {
		_, err := op.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
			Namespace: namespace,
			SearchAttributes: map[string]enumspb.IndexedValueType{
				a.Name: a.Type,
			},
		})
		if err == nil {
			logger.Info("registered search attribute", slog.String("name", a.Name))
			continue
		}
		if isAlreadyExistsError(err) {
			logger.Debug("search attribute already exists", slog.String("name", a.Name))
			continue
		}
		return fmt.Errorf("register %q: %w", a.Name, err)
	}
	return nil
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errors.New("AlreadyExists")) {
		return true
	}
	msg := err.Error()
	return containsFold(msg, "already exists") || containsFold(msg, "AlreadyExists")
}

func containsFold(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
