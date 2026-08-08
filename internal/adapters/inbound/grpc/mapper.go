package grpc

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	loanflowv1 "github.com/smashraid/pandora/gen/go/loanflow/v1"
	"github.com/smashraid/pandora/internal/domain"
)

// ToDomainApplication converts gRPC SubmitApplicationRequest to Domain LoanApplication
func ToDomainApplication(req *loanflowv1.SubmitApplicationRequest) (*domain.LoanApplication, error) {
	docs := make([]domain.Document, 0, len(req.GetDocuments()))
	for _, d := range req.GetDocuments() {
		docs = append(docs, domain.Document{
			ID:    d.GetDocumentId(),
			Type:  d.GetType(),
			S3URL: d.GetS3Url(),
		})
	}

	priority := domain.PriorityStandard
	if req.GetPriority() == loanflowv1.Priority_PRIORITY_HIGH {
		priority = domain.PriorityHigh
	}

	return domain.NewLoanApplication(
		req.GetApplicationId(),
		req.GetApplicantEmail(),
		req.GetRequestedAmountCents(),
		priority,
		docs,
	)
}

// ToProtoSubmitResponse converts Domain ProcessingTask to gRPC SubmitApplicationResponse
func ToProtoSubmitResponse(task *domain.ProcessingTask) *loanflowv1.SubmitApplicationResponse {
	return &loanflowv1.SubmitApplicationResponse{
		TaskId:    task.TaskID,
		Status:    ToProtoTaskStatus(task.Status),
		CreatedAt: timestamppb.New(task.CreatedAt),
	}
}

// ToProtoTrackResponse converts Domain ProcessingTask to gRPC TrackProgressResponse
func ToProtoTrackResponse(task *domain.ProcessingTask) *loanflowv1.TrackProgressResponse {
	return &loanflowv1.TrackProgressResponse{
		TaskId:             task.TaskID,
		Status:             ToProtoTaskStatus(task.Status),
		CurrentStage:       ToProtoProcessingStage(task.CurrentStage),
		ProgressPercentage: task.ProgressPercentage,
		StatusMessage:      task.StatusMessage,
		Timestamp:          timestamppb.New(task.UpdatedAt),
		ErrorDetails:       task.ErrorDetails,
	}
}

func ToProtoTaskStatus(s domain.TaskStatus) loanflowv1.TaskStatus {
	switch s {
	case domain.TaskStatusPending:
		return loanflowv1.TaskStatus_TASK_STATUS_PENDING
	case domain.TaskStatusProcessing:
		return loanflowv1.TaskStatus_TASK_STATUS_PROCESSING
	case domain.TaskStatusCompleted:
		return loanflowv1.TaskStatus_TASK_STATUS_COMPLETED
	case domain.TaskStatusFailed:
		return loanflowv1.TaskStatus_TASK_STATUS_FAILED
	case domain.TaskStatusCancelled:
		return loanflowv1.TaskStatus_TASK_STATUS_CANCELLED
	default:
		return loanflowv1.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

func ToProtoProcessingStage(s domain.ProcessingStage) loanflowv1.ProcessingStage {
	switch s {
	case domain.StageIngestion:
		return loanflowv1.ProcessingStage_PROCESSING_STAGE_INGESTION
	case domain.StageDocumentOCR:
		return loanflowv1.ProcessingStage_PROCESSING_STAGE_DOCUMENT_OCR
	case domain.StageCreditBureauCheck:
		return loanflowv1.ProcessingStage_PROCESSING_STAGE_CREDIT_BUREAU_CHECK
	case domain.StageFraudEvaluation:
		return loanflowv1.ProcessingStage_PROCESSING_STAGE_FRAUD_EVALUATION
	case domain.StageFinalDecision:
		return loanflowv1.ProcessingStage_PROCESSING_STAGE_FINAL_DECISION
	default:
		return loanflowv1.ProcessingStage_PROCESSING_STAGE_UNSPECIFIED
	}
}
