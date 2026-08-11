package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	loanflowv1 "github.com/smashraid/pandora/gen/go/loanflow/v1"
	grpcAdapter "github.com/smashraid/pandora/internal/adapters/inbound/grpc"
	memoryAdapter "github.com/smashraid/pandora/internal/adapters/outbound/memory"
	"github.com/smashraid/pandora/internal/service"
)

const (
	defaultGRPCPort    = ":50051"
	defaultHTTPPort    = ":8080"
	swaggerFilePath    = "gen/openapiv2/loanflow/v1/loanflow.swagger.json"
	shutdownTimeoutSec = 5
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 1. Initialize Outbound Infrastructure Adapters (In-Memory)
	repo := memoryAdapter.NewMemoryTaskRepository()
	broker := memoryAdapter.NewMemoryEventBroker()

	// 2. Initialize Application Service (Core Use Case)
	loanService := service.NewLoanService(repo, broker, broker)

	// 3. Initialize Inbound Delivery Adapter (gRPC Handler)
	grpcHandler := grpcAdapter.NewLoanHandler(loanService, loanService)

	// 4. Create and Start gRPC Server
	lis, err := net.Listen("tcp", defaultGRPCPort) // #nosec G102 -- required for containerized port binding
	if err != nil {
		slog.Error("failed to listen on gRPC port", "port", defaultGRPCPort, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	loanflowv1.RegisterLoanDocumentProcessorServiceServer(grpcServer, grpcHandler)
	reflection.Register(grpcServer)

	go func() {
		slog.Info("Starting LoanFlow Scheduler gRPC Server", "address", defaultGRPCPort)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("gRPC server stopped unexpectedly", "error", err)
		}
	}()

	// 5. Setup gRPC-Gateway HTTP Reverse Proxy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwMux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Local endpoint target for gRPC-Gateway reverse proxy
	grpcEndpoint := "127.0.0.1" + defaultGRPCPort
	if err := loanflowv1.RegisterLoanDocumentProcessorServiceHandlerFromEndpoint(ctx, gwMux, grpcEndpoint, opts); err != nil {
		slog.Error("failed to register gRPC-Gateway handler endpoint", "error", err)
		os.Exit(1)
	}

	// 6. Setup HTTP Router (gRPC-Gateway + OpenAPI Docs)
	httpMux := http.NewServeMux()

	// Serve OpenAPI/Swagger JSON
	httpMux.HandleFunc("/swagger.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.ServeFile(w, r, swaggerFilePath)
	})

	// Mount gRPC-Gateway endpoints to root HTTP handler
	httpMux.Handle("/", gwMux)

	httpServer := &http.Server{
		Addr:              defaultHTTPPort,
		Handler:           httpMux,
		ReadHeaderTimeout: 5 * time.Second,   // Protects against slow request headers
		ReadTimeout:       10 * time.Second,  // Max time to read full request
		WriteTimeout:      10 * time.Second,  // Max time to write response
		IdleTimeout:       120 * time.Second, // Keep-alive connection idle duration
	}

	go func() {
		slog.Info("Starting LoanFlow Scheduler HTTP Gateway & Swagger Server", "address", defaultHTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP gateway server stopped unexpectedly", "error", err)
		}
	}()

	// 7. Graceful Shutdown Signal Handling
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-stopCtx.Done()
	slog.Info("Shutting down servers gracefully...")

	// Shutdown HTTP Gateway
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeoutSec*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP gateway forced shutdown error", "error", err)
	} else {
		slog.Info("HTTP gateway stopped cleanly")
	}

	// Shutdown gRPC Server
	grpcServer.GracefulStop()
	slog.Info("gRPC server stopped cleanly")
}
