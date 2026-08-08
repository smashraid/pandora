package ports

import (
	"context"

	"github.com/smashraid/pandora/internal/domain"
)

type EventPublisher interface {
	PublishProgressEvent(ctx context.Context, task *domain.ProcessingTask) error
}

type EventSubscriber interface {
	SubscribeProgressEvents(ctx context.Context, taskID string) (<-chan *domain.ProcessingTask, error)
}
