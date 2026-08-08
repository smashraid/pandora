package ports

import (
	"context"

	"github.com/smashraid/pandora/internal/domain"
)

type LoanProcessingUseCase interface {
	SubmitApplication(ctx context.Context, app *domain.LoanApplication) (*domain.ProcessingTask, error)
	CancelTask(ctx context.Context, taskID string, reason string) error
	GetTaskStatus(ctx context.Context, taskID string) (*domain.ProcessingTask, error)
	SubscribeTaskUpdates(ctx context.Context, taskID string) (<-chan *domain.ProcessingTask, error)
}
