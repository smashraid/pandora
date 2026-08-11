package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
)

type WorkerProcessor struct {
	repo   outbound.TaskRepository
	queue  outbound.TaskQueue
	pubsub outbound.EventSubscriber // or outbound.PubSub / outbound.EventPublisher depending on port
	logger *slog.Logger
}

func NewWorkerProcessor(
	repo outbound.TaskRepository,
	queue outbound.TaskQueue,
	pubsub outbound.EventSubscriber,
	logger *slog.Logger,
) *WorkerProcessor {
	return &WorkerProcessor{
		repo:   repo,
		queue:  queue,
		pubsub: pubsub,
		logger: logger,
	}
}

// StartWorker loops and dequeues taskIDs for a given priority.
func (wp *WorkerProcessor) StartWorker(ctx context.Context, workerID int, priority domain.Priority) {
	wp.logger.Info("Worker started", "worker_id", workerID, "priority", priority)

	for {
		select {
		case <-ctx.Done():
			wp.logger.Info("Worker shutting down", "worker_id", workerID)
			return
		default:
			// 1. Dequeue taskID from Valkey Priority Queue
			taskID, err := wp.queue.DequeueTask(ctx, priority)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if taskID == "" {
				// Backoff briefly if queue is empty
				time.Sleep(200 * time.Millisecond)
				continue
			}

			// 2. Load Task record from database/repository
			task, err := wp.repo.GetTaskByID(ctx, taskID)
			if err != nil {
				wp.logger.Error("Failed to fetch task from repository", "task_id", taskID, "error", err)
				continue
			}

			if task == nil {
				wp.logger.Warn("Task ID dequeued but not found in repository", "task_id", taskID)
				continue
			}

			// 3. Process Pipeline
			wp.processTask(ctx, workerID, task)
		}
	}
}

func (wp *WorkerProcessor) processTask(ctx context.Context, workerID int, task *domain.ProcessingTask) {
	logger := wp.logger.With("worker_id", workerID, "task_id", task.TaskID)
	logger.Info("Claimed task for processing")

	task.Status = domain.TaskStatusProcessing
	task.CurrentStage = domain.StageIngestion
	task.ProgressPercentage = 5
	task.UpdatedAt = time.Now()

	if err := wp.repo.UpdateTask(ctx, task); err != nil {
		logger.Error("Failed to set task status to PROCESSING", "error", err)
		return
	}

	stages := []struct {
		name    domain.ProcessingStage
		percent int32
		delay   time.Duration
	}{
		{name: domain.StageDocumentOCR, percent: 35, delay: 2 * time.Second},
		{name: domain.StageDocumentValidation, percent: 70, delay: 2 * time.Second},
		{name: domain.StageCreditBureauCheck, percent: 90, delay: 2 * time.Second},
	}

	for _, stage := range stages {
		select {
		case <-ctx.Done():
			logger.Warn("Task pipeline execution interrupted by context cancellation")
			wp.markTaskCancelled(task)
			return
		case <-time.After(stage.delay):
			task.CurrentStage = stage.name
			task.ProgressPercentage = stage.percent
			task.UpdatedAt = time.Now()

			if err := wp.repo.UpdateTask(ctx, task); err != nil {
				logger.Error("Failed to update task stage", "stage", stage.name, "error", err)
			}
			logger.Info("Pipeline stage completed", "stage", stage.name, "progress", stage.percent)
		}
	}

	task.Status = domain.TaskStatusCompleted
	task.CurrentStage = domain.StageFinalDecision
	task.ProgressPercentage = 100
	task.UpdatedAt = time.Now()

	if err := wp.repo.UpdateTask(ctx, task); err != nil {
		logger.Error("Failed to mark task as COMPLETED", "error", err)
	}
	logger.Info("Task processing completed successfully")
}

func (wp *WorkerProcessor) markTaskCancelled(task *domain.ProcessingTask) {
	detachedCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Second)
	defer cancel()

	task.Status = domain.TaskStatusCancelled
	task.CurrentStage = "CANCELLED"
	task.UpdatedAt = time.Now()

	_ = wp.repo.UpdateTask(detachedCtx, task)
}
