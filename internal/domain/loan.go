package domain

import (
	"time"
)

type Priority string

const (
	PriorityUnspecified Priority = "PRIORITY_UNSPECIFIED"
	PriorityStandard    Priority = "PRIORITY_STANDARD"
	PriorityHigh        Priority = "PRIORITY_HIGH"
)

type TaskStatus string

const (
	TaskStatusUnspecified TaskStatus = "TASK_STATUS_UNSPECIFIED"
	TaskStatusPending     TaskStatus = "TASK_STATUS_PENDING"
	TaskStatusProcessing  TaskStatus = "TASK_STATUS_PROCESSING"
	TaskStatusCompleted   TaskStatus = "TASK_STATUS_COMPLETED"
	TaskStatusFailed      TaskStatus = "TASK_STATUS_FAILED"
	TaskStatusCancelled   TaskStatus = "TASK_STATUS_CANCELLED"
)

type ProcessingStage string

const (
	StageUnspecified        ProcessingStage = "PROCESSING_STAGE_UNSPECIFIED"
	StageIngestion          ProcessingStage = "PROCESSING_STAGE_INGESTION"
	StageDocumentOCR        ProcessingStage = "PROCESSING_STAGE_DOCUMENT_OCR"
	StageDocumentValidation ProcessingStage = "PROCESSING_STAGE_DOCUMENT_VALIDATION"
	StageCreditBureauCheck  ProcessingStage = "PROCESSING_STAGE_CREDIT_BUREAU_CHECK"
	StageFraudEvaluation    ProcessingStage = "PROCESSING_STAGE_FRAUD_EVALUATION"
	StageFinalDecision      ProcessingStage = "PROCESSING_STAGE_FINAL_DECISION"
)

type Document struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	S3URL string `json:"s3_url"`
}

type LoanApplication struct {
	ID                   string     `json:"id"`
	ApplicantEmail       string     `json:"applicant_email"`
	RequestedAmountCents int64      `json:"requested_amount_cents"`
	Priority             Priority   `json:"priority"`
	Documents            []Document `json:"documents"`
	CreatedAt            time.Time  `json:"created_at"`
}

type ProcessingTask struct {
	TaskID             string          `json:"task_id"`
	ApplicationID      string          `json:"application_id"`
	Status             TaskStatus      `json:"status"`
	CurrentStage       ProcessingStage `json:"current_stage"`
	ProgressPercentage int32           `json:"progress_percentage"`
	StatusMessage      string          `json:"status_message"`
	ErrorDetails       string          `json:"error_details,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

// NewLoanApplication validates input and returns a new entity.
func NewLoanApplication(id, email string, amount int64, priority Priority, docs []Document) (*LoanApplication, error) {
	if id == "" {
		return nil, ErrInvalidApplicationID
	}
	if email == "" {
		return nil, ErrInvalidEmail
	}
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if len(docs) == 0 {
		return nil, ErrNoDocuments
	}

	return &LoanApplication{
		ID:                   id,
		ApplicantEmail:       email,
		RequestedAmountCents: amount,
		Priority:             priority,
		Documents:            docs,
		CreatedAt:            time.Now().UTC(),
	}, nil
}
