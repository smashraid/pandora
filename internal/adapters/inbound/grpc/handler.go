package grpc

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	loanflowv1 "github.com/smashraid/pandora/gen/go/loanflow/v1"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/inbound"
)

type LoanHandler struct {
	loanflowv1.UnimplementedLoanDocumentProcessorServiceServer
	submitUC inbound.SubmitLoanUseCase
	trackUC  inbound.TrackProgressUseCase
}

func NewLoanHandler(submitUC inbound.SubmitLoanUseCase, trackUC inbound.TrackProgressUseCase) *LoanHandler {
	return &LoanHandler{
		submitUC: submitUC,
		trackUC:  trackUC,
	}
}

// SubmitApplication (Unary RPC)
func (h *LoanHandler) SubmitApplication(ctx context.Context, req *loanflowv1.SubmitApplicationRequest) (*loanflowv1.SubmitApplicationResponse, error) {
	app, err := ToDomainApplication(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid application: %v", err)
	}

	task, err := h.submitUC.SubmitApplication(ctx, app)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to submit application: %v", err)
	}

	return ToProtoSubmitResponse(task), nil
}

// TrackProgress (Bidirectional Streaming RPC)
func (h *LoanHandler) TrackProgress(stream loanflowv1.LoanDocumentProcessorService_TrackProgressServer) error {
	ctx := stream.Context()

	// Wait for the initial connection frame to obtain subscription task_id
	firstReq, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to receive initial subscription frame: %v", err)
	}

	subCmd := firstReq.GetSubscribe()
	if subCmd == nil || subCmd.GetTaskId() == "" {
		return status.Errorf(codes.InvalidArgument, "first stream payload must contain a valid subscribe command")
	}

	taskID := subCmd.GetTaskId()

	// 1. Send immediate current status update
	initialTask, err := h.trackUC.GetTaskStatus(ctx, taskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			return status.Errorf(codes.NotFound, "task %s not found", taskID)
		}
		return status.Errorf(codes.Internal, "failed to query task status: %v", err)
	}

	if err := stream.Send(ToProtoTrackResponse(initialTask)); err != nil {
		return err
	}

	// 2. Subscribe to progress updates channel
	updatesChan, err := h.trackUC.SubscribeTaskUpdates(ctx, taskID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe to task updates: %v", err)
	}

	// Channel to signal client cancellation or error from the inbound stream reader
	errChan := make(chan error, 1)

	// Goroutine: Listen for client mid-flight CancelCommand requests on incoming stream
	go func() {
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				errChan <- err
				return
			}

			if cancelCmd := req.GetCancel(); cancelCmd != nil {
				_ = h.trackUC.CancelTask(ctx, cancelCmd.GetTaskId(), cancelCmd.GetReason())
			}
		}
	}()

	// Stream Loop: Forward real-time task progress events to client
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			return err
		case task, ok := <-updatesChan:
			if !ok {
				return nil
			}
			if err := stream.Send(ToProtoTrackResponse(task)); err != nil {
				return err
			}
			if task.IsTerminal() {
				return nil
			}
		}
	}
}
