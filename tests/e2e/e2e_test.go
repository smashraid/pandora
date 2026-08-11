package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	loanflowv1 "github.com/smashraid/pandora/gen/go/loanflow/v1"
	grpcAdapter "github.com/smashraid/pandora/internal/adapters/inbound/grpc"
	postgresAdapter "github.com/smashraid/pandora/internal/adapters/outbound/postgres"
	valkeyAdapter "github.com/smashraid/pandora/internal/adapters/outbound/valkey"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/service"
	"github.com/smashraid/pandora/pkg/config"
)

func TestE2E_FullLoanProcessingPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Load Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// 2. Initialize PostgreSQL
	db, err := sql.Open(cfg.Database.DriverName, cfg.Database.DatabaseURL)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("database ping failed: %v", err)
	}

	// 3. Initialize Valkey
	vkAdapter, err := valkeyAdapter.NewValkeyAdapter(cfg.Valkey)
	if err != nil {
		t.Fatalf("failed to connect to valkey: %v", err)
	}

	// 4. Initialize Core Services
	repo := postgresAdapter.NewPostgresTaskRepository(db)
	loanService := service.NewLoanService(repo, vkAdapter, vkAdapter)

	// 5. Start In-Memory gRPC Server on Ephemeral Port
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on ephemeral port: %v", err)
	}
	grpcAddr := grpcLis.Addr().String()

	grpcServer := grpc.NewServer()
	grpcHandler := grpcAdapter.NewLoanHandler(loanService, loanService)
	loanflowv1.RegisterLoanDocumentProcessorServiceServer(grpcServer, grpcHandler)

	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
	})

	// 6. Start gRPC-Gateway HTTP Server on Ephemeral Port
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on http port: %v", err)
	}
	httpAddr := httpLis.Addr().String()

	gwMux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := loanflowv1.RegisterLoanDocumentProcessorServiceHandlerFromEndpoint(ctx, gwMux, grpcAddr, opts); err != nil {
		t.Fatalf("failed to register gateway endpoint: %v", err)
	}

	httpServer := &http.Server{
		Handler: gwMux,
	}

	go func() {
		_ = httpServer.Serve(httpLis)
	}()
	t.Cleanup(func() {
		_ = httpServer.Shutdown(context.Background())
	})

	// 7. Start Background Worker Processor
	logger := service.NewWorkerProcessor(repo, vkAdapter, vkAdapter, nil) // uses slog default
	go logger.StartWorker(ctx, 1, domain.PriorityHigh)
	go logger.StartWorker(ctx, 2, domain.PriorityStandard)

	// =========================================================================
	// STEP 1: Submit Application via HTTP Gateway (REST API)
	// =========================================================================
	appID := fmt.Sprintf("e2e-app-%d", time.Now().UnixNano())
	payload := map[string]any{
		"application_id":         appID,
		"applicant_email":        "e2e.tester@example.com",
		"requested_amount_cents": 50000000, // $500,000.00 -> High Priority
		"priority":               "PRIORITY_HIGH",
		"documents": []map[string]string{
			{
				"document_id": "doc-001",
				"type":        "PAYSLIP",
				"s3_url":      "https://s3.amazonaws.com/bucket/payslip.pdf",
			},
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/applications", httpAddr), "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("HTTP POST /v1/applications failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected HTTP 200, got %d: %s", resp.StatusCode, string(respBytes))
	}

	var submitResp struct {
		TaskId    string `json:"taskId"`
		Status    string `json:"status"`
		CreatedAt string `json:"createdAt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&submitResp); err != nil {
		t.Fatalf("failed to parse submit response: %v", err)
	}

	if submitResp.TaskId == "" {
		t.Fatal("received empty task_id from application submission")
	}

	t.Logf("[STEP 1 PASS] Application %s submitted. Task ID: %s", appID, submitResp.TaskId)

	// =========================================================================
	// STEP 2: Stream Task Progress via gRPC Stream (TrackProgress)
	// =========================================================================
	grpcConn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}
	defer grpcConn.Close()

	client := loanflowv1.NewLoanDocumentProcessorServiceClient(grpcConn)
	stream, err := client.TrackProgress(ctx)
	if err != nil {
		t.Fatalf("failed to open TrackProgress stream: %v", err)
	}

	// Send Subscribe Frame
	err = stream.Send(&loanflowv1.TrackProgressRequest{
		Payload: &loanflowv1.TrackProgressRequest_Subscribe{
			Subscribe: &loanflowv1.SubscribeCommand{TaskId: submitResp.TaskId},
		},
	})
	if err != nil {
		t.Fatalf("failed to send subscribe frame: %v", err)
	}

	// Receive progress updates until terminal state
	var finalResponse *loanflowv1.TrackProgressResponse
	progressUpdatesReceived := 0

	for {
		update, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error receiving progress update stream: %v", err)
		}

		progressUpdatesReceived++
		t.Logf("  [STREAM EVENT] Task: %s | Stage: %s | Progress: %d%%",
			update.GetTaskId(), update.GetCurrentStage().String(), update.GetProgressPercentage())

		if update.GetStatus() == loanflowv1.TaskStatus_TASK_STATUS_COMPLETED ||
			update.GetStatus() == loanflowv1.TaskStatus_TASK_STATUS_FAILED ||
			update.GetStatus() == loanflowv1.TaskStatus_TASK_STATUS_CANCELLED {
			finalResponse = update
			break
		}
	}

	if finalResponse == nil {
		t.Fatal("stream ended without receiving terminal task status")
	}

	if finalResponse.GetStatus() != loanflowv1.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("expected task status COMPLETED, got: %s (error: %s)",
			finalResponse.GetStatus().String(), finalResponse.GetErrorDetails())
	}

	if finalResponse.GetProgressPercentage() != 100 {
		t.Errorf("expected 100%% progress percentage, got %d%%", finalResponse.GetProgressPercentage())
	}

	t.Logf("[STEP 2 PASS] TrackProgress stream verified (%d events received). Final stage: %s",
		progressUpdatesReceived, finalResponse.GetCurrentStage().String())

	// =========================================================================
	// STEP 3: Verify Persistence in PostgreSQL Database
	// =========================================================================
	dbTask, err := repo.GetTaskByID(ctx, submitResp.TaskId)
	if err != nil {
		t.Fatalf("failed to fetch task from database: %v", err)
	}

	if dbTask.Status != domain.TaskStatusCompleted {
		t.Errorf("database task status mismatch: expected %s, got %s", domain.TaskStatusCompleted, dbTask.Status)
	}

	if dbTask.ProgressPercentage != 100 {
		t.Errorf("database progress mismatch: expected 100, got %d", dbTask.ProgressPercentage)
	}

	t.Logf("[STEP 3 PASS] PostgreSQL persistence verified for Task ID: %s", submitResp.TaskId)
}
