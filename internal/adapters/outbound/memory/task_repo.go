package memory

import (
	"context"
	"sync"

	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
)

var _ outbound.TaskRepository = (*MemoryTaskRepository)(nil)

type MemoryTaskRepository struct {
	mu           sync.RWMutex
	applications map[string]*domain.LoanApplication
	tasks        map[string]*domain.ProcessingTask
}

func NewMemoryTaskRepository() *MemoryTaskRepository {
	return &MemoryTaskRepository{
		applications: make(map[string]*domain.LoanApplication),
		tasks:        make(map[string]*domain.ProcessingTask),
	}
}

func (r *MemoryTaskRepository) SaveApplication(ctx context.Context, app *domain.LoanApplication) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	appCopy := *app
	r.applications[app.ID] = &appCopy
	return nil
}

func (r *MemoryTaskRepository) GetApplicationByID(ctx context.Context, appID string) (*domain.LoanApplication, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	app, ok := r.applications[appID]
	if !ok {
		return nil, domain.ErrTaskNotFound
	}
	appCopy := *app
	return &appCopy, nil
}

func (r *MemoryTaskRepository) CreateTask(ctx context.Context, task *domain.ProcessingTask) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	taskCopy := *task
	r.tasks[task.TaskID] = &taskCopy
	return nil
}

func (r *MemoryTaskRepository) GetTaskByID(ctx context.Context, taskID string) (*domain.ProcessingTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	task, ok := r.tasks[taskID]
	if !ok {
		return nil, domain.ErrTaskNotFound
	}
	taskCopy := *task
	return &taskCopy, nil
}

func (r *MemoryTaskRepository) UpdateTask(ctx context.Context, task *domain.ProcessingTask) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.tasks[task.TaskID]; !ok {
		return domain.ErrTaskNotFound
	}

	taskCopy := *task
	r.tasks[task.TaskID] = &taskCopy
	return nil
}
