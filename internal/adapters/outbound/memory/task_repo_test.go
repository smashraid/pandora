package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/smashraid/pandora/internal/adapters/outbound/memory"
	"github.com/smashraid/pandora/internal/domain"
)

func TestMemoryTaskRepository_ApplicationOperations(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryTaskRepository()

	app := &domain.LoanApplication{
		ID:                   "app-100",
		ApplicantEmail:       "test@example.com",
		RequestedAmountCents: 5000000,
		Priority:             domain.PriorityHigh,
		CreatedAt:            time.Now().UTC(),
	}

	// 1. Save Application
	if err := repo.SaveApplication(ctx, app); err != nil {
		t.Fatalf("expected no error saving application, got: %v", err)
	}

	// 2. Get Application Success
	fetched, err := repo.GetApplicationByID(ctx, "app-100")
	if err != nil {
		t.Fatalf("expected application, got error: %v", err)
	}
	if fetched.ApplicantEmail != app.ApplicantEmail {
		t.Errorf("expected email %s, got %s", app.ApplicantEmail, fetched.ApplicantEmail)
	}

	// 3. Get Application NotFound
	_, err = repo.GetApplicationByID(ctx, "non-existent")
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("expected domain.ErrTaskNotFound, got: %v", err)
	}
}

func TestMemoryTaskRepository_TaskOperations(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryTaskRepository()

	task := &domain.ProcessingTask{
		TaskID:             "task-100",
		ApplicationID:      "app-100",
		Status:             domain.TaskStatusPending,
		CurrentStage:       domain.StageIngestion,
		ProgressPercentage: 0,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}

	// 1. Create Task
	if err := repo.CreateTask(ctx, task); err != nil {
		t.Fatalf("expected no error creating task, got: %v", err)
	}

	// 2. Get Task
	fetched, err := repo.GetTaskByID(ctx, "task-100")
	if err != nil {
		t.Fatalf("expected task, got error: %v", err)
	}
	if fetched.TaskID != task.TaskID {
		t.Errorf("expected task ID %s, got %s", task.TaskID, fetched.TaskID)
	}

	// 3. Update Task
	task.Status = domain.TaskStatusProcessing
	task.ProgressPercentage = 35
	if err := repo.UpdateTask(ctx, task); err != nil {
		t.Fatalf("expected no error updating task, got: %v", err)
	}

	updated, err := repo.GetTaskByID(ctx, "task-100")
	if err != nil {
		t.Fatalf("expected updated task, got error: %v", err)
	}
	if updated.ProgressPercentage != 35 {
		t.Errorf("expected progress 35, got %d", updated.ProgressPercentage)
	}

	// 4. Update Non-Existent Task
	invalidTask := &domain.ProcessingTask{TaskID: "invalid"}
	err = repo.UpdateTask(ctx, invalidTask)
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Errorf("expected domain.ErrTaskNotFound updating missing task, got: %v", err)
	}
}
