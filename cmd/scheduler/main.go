package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	loanflowv1 "github.com/smashraid/pandora/gen/go/loanflow/v1"
	grpcAdapter "github.com/smashraid/pandora/internal/adapters/inbound/grpc"
	memoryAdapter "github.com/smashraid/pandora/internal/adapters/outbound/memory"
	"github.com/smashraid/pandora/internal/services"
)

const defaultPort = ":50051"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 1. Initialize Outbound Infrastructure Adapters (In-Memory for now)
	repo := memoryAdapter.NewMemoryTaskRepository()
	broker := memoryAdapter.NewEventBroker()

	// 2. Initialize Application Service (Core Use Case)
	loanService := services.NewLoanService(repo, broker, broker)

	// 3. Initialize Inbound Delivery Adapter (gRPC Handler)
	grpcHandler := grpcAdapter.NewLoanHandler(loanService, loanService)

	// 4. Create and Configure gRPC Server
	lis, err := net.Listen("tcp", defaultPort)
	if err != nil {
		slog.Error("failed to listen on port", "port", defaultPort, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	loanflowv1.RegisterLoanDocumentProcessorServiceServer(grpcServer, grpcHandler)

	// Enable gRPC Server Reflection for testing with grpcurl or Postman
	reflection.Register(grpcServer)

	// 5. Graceful Shutdown Signal Handling
	stopCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		slog.Info("Starting LoanFlow Scheduler gRPC Server", "address", defaultPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server stopped with error", "error", err)
		}
	}()

	<-stopCtx.Done()
	slog.Info("Shutting down gRPC server gracefully...")
	grpcServer.GracefulStop()
	slog.Info("Server stopped successfully")
}
