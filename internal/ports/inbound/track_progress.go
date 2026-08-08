// internal/ports/inbound/track_progress.go
package inbound

import (
	"context"

	"github.com/smashraid/pandora/internal/domain"
)

type TrackProgressUseCase interface {
	CancelTask(ctx context.Context, taskID string, reason string) error
	GetTaskStatus(ctx context.Context, taskID string) (*domain.ProcessingTask, error)
	SubscribeTaskUpdates(ctx context.Context, taskID string) (<-chan *domain.ProcessingTask, error)
}
