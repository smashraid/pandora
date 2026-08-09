package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/smashraid/pandora/internal/adapters/outbound/memory"
	"github.com/smashraid/pandora/internal/domain"
)

func TestMemoryTaskRepository_SaveAndGetTask(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryTaskRepository()

	task := &domain.ProcessingTask{
		TaskID:             "task-123",
		ApplicationID:      "app-456",
		Status:             domain.TaskStatusPending,
		CurrentStage:       domain.StageIngestion,
		ProgressPercentage: 0,
		StatusMessage:      "Task initialized",
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}

	// 1. Create Task
	err := repo.CreateTask(ctx, task)
	if err != nil {
		t.Fatalf("expected no error saving task, got: %v", err)
	}

	// 2. Get Task (Success Case)
	fetched, err := repo.GetTaskByID(ctx, "task-123")
	if err != nil {
		t.Fatalf("expected to find task, got: %v", err)
	}

	if fetched.TaskID != task.TaskID {
		t.Errorf("expected task ID %s, got %s", task.TaskID, fetched.TaskID)
	}
	if fetched.Status != domain.TaskStatusPending {
		t.Errorf("expected status %s, got %s", domain.TaskStatusPending, fetched.Status)
	}

	// 3. Get Task (NotFound Case)
	_, err = repo.GetTaskByID(ctx, "non-existent-id")
	if err == nil {
		t.Error("expected error for non-existent task, got nil")
	}

}

func TestMemoryTaskRepository_UpdateTaskStatus(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewMemoryTaskRepository()

	task := &domain.ProcessingTask{
		TaskID:             "task-999",
		ApplicationID:      "app-999",
		Status:             domain.TaskStatusPending,
		CurrentStage:       domain.StageIngestion,
		ProgressPercentage: 10,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}

	_ = repo.CreateTask(ctx, task)

	// Mutate task
	task.Status = domain.TaskStatusProcessing
	task.CurrentStage = domain.StageDocumentOCR
	task.ProgressPercentage = 35

	err := repo.UpdateTask(ctx, task)
	if err != nil {
		t.Fatalf("expected no error updating task, got: %v", err)
	}

	fetched, err := repo.GetTaskByID(ctx, "task-999")
	if err != nil {
		t.Fatalf("expected to find updated task, got: %v", err)
	}

	if fetched.ProgressPercentage != 35 {
		t.Errorf("expected progress 35%%, got %d%%", fetched.ProgressPercentage)
	}
	if fetched.CurrentStage != domain.StageDocumentOCR {
		t.Errorf("expected stage %s, got %s", domain.StageDocumentOCR, fetched.CurrentStage)
	}
}
