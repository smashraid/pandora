package outbound

import (
	"context"

	"github.com/smashraid/pandora/internal/domain"
)

type TaskQueue interface {
	EnqueueTask(ctx context.Context, taskID string, priority domain.Priority) error
	DequeueTask(ctx context.Context, priority domain.Priority) (string, error)
}
