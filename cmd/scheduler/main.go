package main

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/smashraid/pandora/api/loanflow/v1"
)

type server struct {
	pb.UnimplementedLoanServiceServer
	// Store active streams to allow cancellation
	mu      sync.RWMutex
	streams map[string]chan bool // task_id -> cancel channel
}

func (s *server) SubmitApplication(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	log.Printf("Received application: %s", req.ApplicationId)
	// Dummy task ID generation
	taskID := "task-" + req.ApplicationId
	return &pb.SubmitResponse{TaskId: taskID}, nil
}

func (s *server) TrackProgress(stream pb.LoanService_TrackProgressServer) error {
	// First message must contain task_id
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Unknown, "failed to receive initial request: %v", err)
	}
	taskID := req.TaskId
	log.Printf("TrackProgress started for task: %s", taskID)

	// Create cancel channel for this stream
	cancelChan := make(chan bool)
	s.mu.Lock()
	if s.streams == nil {
		s.streams = make(map[string]chan bool)
	}
	s.streams[taskID] = cancelChan
	s.mu.Unlock()

	// Clean up when function exits
	defer func() {
		s.mu.Lock()
		delete(s.streams, taskID)
		s.mu.Unlock()
		close(cancelChan)
	}()

	// Goroutine to handle incoming messages (e.g., cancel request)
	go func() {
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Printf("Error receiving from stream: %v", err)
				return
			}
			if req.Cancel {
				log.Printf("Cancel requested for task: %s", taskID)
				cancelChan <- true
				return
			}
		}
	}()

	// Simulate progress steps
	steps := []struct {
		step    string
		percent int
		message string
		delay   time.Duration
	}{
		{"OCR", 10, "Starting OCR on document...", 1 * time.Second},
		{"OCR", 50, "Processing page 5/10...", 2 * time.Second},
		{"OCR", 100, "OCR completed", 1 * time.Second},
		{"Validation", 20, "Validating signatures...", 2 * time.Second},
		{"Validation", 100, "Validation passed", 1 * time.Second},
		{"CreditCheck", 30, "Calling credit bureau...", 3 * time.Second},
		{"CreditCheck", 100, "Credit score retrieved", 1 * time.Second},
		{"Done", 100, "Loan application processed successfully", 0},
	}

	for _, st := range steps {
		// Check if cancellation was requested
		select {
		case <-cancelChan:
			log.Printf("Task %s cancelled by client", taskID)
			return status.Errorf(codes.Canceled, "task cancelled by client")
		default:
		}

		// Send progress event
		event := &pb.ProgressEvent{
			TaskId:  taskID,
			Step:    st.step,
			Percent: int32(st.percent),
			Message: st.message,
		}
		if err := stream.Send(event); err != nil {
			return err
		}
		time.Sleep(st.delay)
	}

	log.Printf("Task %s completed successfully", taskID)
	return nil
}

func main() {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterLoanServiceServer(s, &server{})
	reflection.Register(s)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
