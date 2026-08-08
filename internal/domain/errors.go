package domain

import "errors"

var (
	ErrInvalidApplicationID = errors.New("application_id must not be empty")
	ErrInvalidEmail         = errors.New("applicant_email is invalid")
	ErrInvalidAmount        = errors.New("requested_amount_cents must be greater than zero")
	ErrNoDocuments          = errors.New("at least one document is required")
	ErrInvalidTransition    = errors.New("invalid task status transition")
	ErrTaskNotFound         = errors.New("loan processing task not found")
	ErrTaskAlreadyCompleted = errors.New("task is already in a terminal state")
	ErrTaskCancelled        = errors.New("task was cancelled by user")
)
