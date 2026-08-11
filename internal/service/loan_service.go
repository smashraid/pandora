package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
)

type LoanService struct {
	repo       outbound.TaskRepository
	publisher  outbound.EventPublisher
	subscriber outbound.EventSubscriber
}

func NewLoanService(repo outbound.TaskRepository, pub outbound.EventPublisher, sub outbound.EventSubscriber) *LoanService {
	return &LoanService{
		repo:       repo,
		publisher:  pub,
		subscriber: sub,
	}
}

func (s *LoanService) SubmitApplication(ctx context.Context, app *domain.LoanApplication) (*domain.ProcessingTask, error) {
	if err := s.repo.SaveApplication(ctx, app); err != nil {
		return nil, fmt.Errorf("failed to save application: %w", err)
	}

	task := &domain.ProcessingTask{
		TaskID:             uuid.NewString(),
		ApplicationID:      app.ID,
		Status:             domain.TaskStatusPending,
		CurrentStage:       domain.StageIngestion,
		ProgressPercentage: 0,
		StatusMessage:      "Application received and queued for processing",
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}

	if err := s.repo.CreateTask(ctx, task); err != nil {
		return nil, fmt.Errorf("failed to create processing task: %w", err)
	}

	// Trigger async pipeline processing
	go s.processPipeline(context.WithoutCancel(ctx), task.TaskID)

	return task, nil
}

func (s *LoanService) CancelTask(ctx context.Context, taskID string, reason string) error {
	task, err := s.repo.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}

	if task.IsTerminal() {
		return domain.ErrTaskAlreadyCompleted
	}

	msg := fmt.Sprintf("Cancelled by user: %s", reason)
	if err := task.TransitionTo(domain.TaskStatusCancelled, task.CurrentStage, task.ProgressPercentage, msg); err != nil {
		return err
	}

	if err := s.repo.UpdateTask(ctx, task); err != nil {
		return err
	}

	_ = s.publisher.PublishProgressEvent(ctx, task)
	return nil
}

func (s *LoanService) GetTaskStatus(ctx context.Context, taskID string) (*domain.ProcessingTask, error) {
	return s.repo.GetTaskByID(ctx, taskID)
}

func (s *LoanService) SubscribeTaskUpdates(ctx context.Context, taskID string) (<-chan *domain.ProcessingTask, error) {
	return s.subscriber.SubscribeProgressEvents(ctx, taskID)
}

// processPipeline simulates the asynchronous document processing steps
func (s *LoanService) processPipeline(ctx context.Context, taskID string) {
	stages := []struct {
		stage    domain.ProcessingStage
		progress int32
		msg      string
		duration time.Duration
	}{
		{domain.StageIngestion, 10, "Ingesting application payload and documents", 500 * time.Millisecond},
		{domain.StageDocumentOCR, 35, "Performing document OCR scanning", 800 * time.Millisecond},
		{domain.StageCreditBureauCheck, 65, "Querying credit bureau background check", 800 * time.Millisecond},
		{domain.StageFraudEvaluation, 85, "Evaluating anti-fraud risk models", 600 * time.Millisecond},
		{domain.StageFinalDecision, 100, "Loan processing complete and approved", 400 * time.Millisecond},
	}

	for _, step := range stages {
		time.Sleep(step.duration)

		task, err := s.repo.GetTaskByID(ctx, taskID)
		if err != nil || task.IsTerminal() {
			// Task was cancelled or failed externally
			return
		}

		nextStatus := domain.TaskStatusProcessing
		if step.progress == 100 {
			nextStatus = domain.TaskStatusCompleted
		}

		if err := task.TransitionTo(nextStatus, step.stage, step.progress, step.msg); err != nil {
			return
		}

		_ = s.repo.UpdateTask(ctx, task)
		_ = s.publisher.PublishProgressEvent(ctx, task)
	}
}
