package domain

import (
	"fmt"
	"time"
)

// TransitionMap defines valid next states for a given status.
var allowedTransitions = map[TaskStatus]map[TaskStatus]bool{
	TaskStatusPending: {
		TaskStatusProcessing: true,
		TaskStatusCancelled:  true,
		TaskStatusFailed:     true,
	},
	TaskStatusProcessing: {
		TaskStatusProcessing: true, // Stage updates while remaining in processing
		TaskStatusCompleted:  true,
		TaskStatusFailed:     true,
		TaskStatusCancelled:  true,
	},
	TaskStatusCompleted: {}, // Terminal state
	TaskStatusFailed:    {}, // Terminal state
	TaskStatusCancelled: {}, // Terminal state
}

// TransitionTo updates the task's status and stage according to state machine rules.
func (t *ProcessingTask) TransitionTo(nextStatus TaskStatus, stage ProcessingStage, progress int32, msg string) error {
	if !allowedTransitions[t.Status][nextStatus] {
		return fmt.Errorf("%w: cannot move from %s to %s", ErrInvalidTransition, t.Status, nextStatus)
	}

	if progress < 0 || progress > 100 {
		progress = t.ProgressPercentage
	}

	t.Status = nextStatus
	t.CurrentStage = stage
	t.ProgressPercentage = progress
	t.StatusMessage = msg
	t.UpdatedAt = time.Now().UTC()

	return nil
}

// IsTerminal returns true if the task cannot undergo further state changes.
func (t *ProcessingTask) IsTerminal() bool {
	return t.Status == TaskStatusCompleted || t.Status == TaskStatusFailed || t.Status == TaskStatusCancelled
}
