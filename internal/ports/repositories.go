package ports

import (
	"context"

	"github.com/smashraid/pandora/internal/domain"
)

type TaskRepository interface {
	SaveApplication(ctx context.Context, app *domain.LoanApplication) error
	GetApplicationByID(ctx context.Context, appID string) (*domain.LoanApplication, error)

	CreateTask(ctx context.Context, task *domain.ProcessingTask) error
	GetTaskByID(ctx context.Context, taskID string) (*domain.ProcessingTask, error)
	UpdateTask(ctx context.Context, task *domain.ProcessingTask) error
}
