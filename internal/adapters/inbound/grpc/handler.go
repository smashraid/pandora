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

	// 1. Wait for initial subscription frame from client
	firstReq, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "stream closed before sending subscription frame")
		}
		return status.Errorf(codes.InvalidArgument, "failed to receive subscription frame: %v", err)
	}

	subCmd := firstReq.GetSubscribe()
	if subCmd == nil || subCmd.GetTaskId() == "" {
		return status.Error(codes.InvalidArgument, "first stream payload must contain a valid subscribe command")
	}

	taskID := subCmd.GetTaskId()

	// 2. Fetch and send immediate current task status
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

	// 3. Early return if task is already in a terminal state
	if initialTask.IsTerminal() {
		return nil
	}

	// 4. Subscribe to PubSub task updates channel
	updatesChan, err := h.trackUC.SubscribeTaskUpdates(ctx, taskID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe to task updates: %v", err)
	}

	// Channel to signal client cancellation or error from the inbound stream listener
	errChan := make(chan error, 1)

	// 5. Goroutine: Non-blocking client command listener (handles mid-flight CancelCommand)
	go func() {
		for {
			req, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				// Client half-closed (stopped sending commands). Keep pushing server updates.
				return
			}
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
				return
			}

			// Process mid-flight cancellation request from client
			if cancelCmd := req.GetCancel(); cancelCmd != nil {
				_ = h.trackUC.CancelTask(ctx, cancelCmd.GetTaskId(), cancelCmd.GetReason())
			}
		}
	}()

	// 6. Main Server Event Loop: Stream updates to client until task terminates or stream closes
	for {
		select {
		case <-ctx.Done():
			return status.Error(codes.Canceled, "stream context canceled by client")

		case err := <-errChan:
			return status.Errorf(codes.Canceled, "client stream error: %v", err)

		case task, ok := <-updatesChan:
			if !ok {
				return nil // Subscription channel closed
			}

			if err := stream.Send(ToProtoTrackResponse(task)); err != nil {
				return err
			}

			if task.IsTerminal() {
				return nil // Processing completed/failed/cancelled; cleanly end stream
			}
		}
	}
}
