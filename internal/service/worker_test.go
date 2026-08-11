package service_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	memoryAdapter "github.com/smashraid/pandora/internal/adapters/outbound/memory"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/service"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWorkerProcessor_SuccessfulPipelineExecution(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repo := memoryAdapter.NewMemoryTaskRepository()
	broker := memoryAdapter.NewMemoryEventBroker()
	worker := service.NewWorkerProcessor(repo, broker, broker, newTestLogger())

	// 1. Create and seed initial processing task
	taskID := "task-worker-test-001"
	appID := "app-worker-test-001"
	now := time.Now()

	task := &domain.ProcessingTask{
		TaskID:             taskID,
		ApplicationID:      appID,
		Status:             domain.TaskStatusPending,
		CurrentStage:       domain.StageIngestion,
		ProgressPercentage: 0,
		StatusMessage:      "Queued for processing",
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	if err := repo.CreateTask(ctx, task); err != nil {
		t.Fatalf("failed to create task in repository: %v", err)
	}

	if err := broker.EnqueueTask(ctx, taskID, domain.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue task: %v", err)
	}

	// 2. Run worker in background goroutine
	go worker.StartWorker(ctx, 1, domain.PriorityHigh)

	// 3. Poll repository until task completes or timeout occurs
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for worker to complete processing task")
		case <-ticker.C:
			updatedTask, err := repo.GetTaskByID(ctx, taskID)
			if err != nil {
				t.Fatalf("failed to get task: %v", err)
			}

			if updatedTask.Status == domain.TaskStatusCompleted {
				if updatedTask.ProgressPercentage != 100 {
					t.Errorf("expected 100%% progress, got %d%%", updatedTask.ProgressPercentage)
				}
				if updatedTask.CurrentStage != domain.StageFinalDecision {
					t.Errorf("expected stage %s, got %s", domain.StageFinalDecision, updatedTask.CurrentStage)
				}
				return // Success
			}
		}
	}
}

func TestWorkerProcessor_ContextCancellationDuringPipeline(t *testing.T) {
	t.Parallel()

	// Context will be cancelled mid-pipeline (after stage 1 begins)
	workerCtx, cancelWorker := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelWorker()

	repo := memoryAdapter.NewMemoryTaskRepository()
	broker := memoryAdapter.NewMemoryEventBroker()
	worker := service.NewWorkerProcessor(repo, broker, broker, newTestLogger())

	taskID := "task-worker-cancel-001"
	task := &domain.ProcessingTask{
		TaskID:             taskID,
		ApplicationID:      "app-cancel-001",
		Status:             domain.TaskStatusPending,
		CurrentStage:       domain.StageIngestion,
		ProgressPercentage: 0,
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
	}

	if err := repo.CreateTask(context.Background(), task); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if err := broker.EnqueueTask(context.Background(), taskID, domain.PriorityStandard); err != nil {
		t.Fatalf("failed to enqueue task: %v", err)
	}

	// Run worker with the short context
	go worker.StartWorker(workerCtx, 1, domain.PriorityStandard)

	// Wait for context cancellation to trigger cleanup
	<-workerCtx.Done()
	time.Sleep(200 * time.Millisecond) // Allow detached cancellation cleanup goroutine to finish

	updatedTask, err := repo.GetTaskByID(context.Background(), taskID)
	if err != nil {
		t.Fatalf("failed to query task status post-cancellation: %v", err)
	}

	if updatedTask.Status != domain.TaskStatusCancelled {
		t.Errorf("expected status %s, got %s", domain.TaskStatusCancelled, updatedTask.Status)
	}
}

func TestWorkerProcessor_MissingTaskIDInRepo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	repo := memoryAdapter.NewMemoryTaskRepository()
	broker := memoryAdapter.NewMemoryEventBroker()
	worker := service.NewWorkerProcessor(repo, broker, broker, newTestLogger())

	// Enqueue task ID that doesn't exist in repo
	if err := broker.EnqueueTask(ctx, "non-existent-task-id", domain.PriorityStandard); err != nil {
		t.Fatalf("failed to enqueue task: %v", err)
	}

	// Worker should handle missing task gracefully without crashing
	go worker.StartWorker(ctx, 1, domain.PriorityStandard)

	<-ctx.Done() // Assert worker shuts down cleanly
}

func TestWorkerProcessor_EmptyQueueShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	repo := memoryAdapter.NewMemoryTaskRepository()
	broker := memoryAdapter.NewMemoryEventBroker()
	worker := service.NewWorkerProcessor(repo, broker, broker, newTestLogger())

	// Worker loops on empty queue and exits cleanly on context cancellation
	done := make(chan struct{})
	go func() {
		worker.StartWorker(ctx, 1, domain.PriorityStandard)
		close(done)
	}()

	select {
	case <-done:
		// Success: worker stopped when context expired
	case <-time.After(1 * time.Second):
		t.Fatal("worker failed to shut down on context cancellation")
	}
}
