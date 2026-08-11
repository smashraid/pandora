package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	loanflowv1 "github.com/smashraid/pandora/gen/go/loanflow/v1"
	grpcAdapter "github.com/smashraid/pandora/internal/adapters/inbound/grpc"
	postgresAdapter "github.com/smashraid/pandora/internal/adapters/outbound/postgres"
	valkeyAdapter "github.com/smashraid/pandora/internal/adapters/outbound/valkey"
	"github.com/smashraid/pandora/internal/service"
	"github.com/smashraid/pandora/pkg/config"
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

	// 1. Load Application Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// 2. Initialize PostgreSQL Connection
	db, err := sql.Open(cfg.Database.DriverName, cfg.Database.DatabaseURL)
	if err != nil {
		slog.Error("failed to open database connection", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)

	dbPingCtx, dbPingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(dbPingCtx); err != nil {
		dbPingCancel()
		slog.Error("failed to connect to postgres database", "error", err)
		os.Exit(1)
	}
	dbPingCancel()
	slog.Info("Connected to PostgreSQL database")

	// 3. Initialize Valkey Queue & Event Adapter
	vkAdapter, err := valkeyAdapter.NewValkeyAdapter(cfg.Valkey)
	if err != nil {
		slog.Error("failed to initialize valkey adapter", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to Valkey instance", "addr", cfg.Valkey.Addr)

	// 4. Initialize Outbound Infrastructure Repositories & Domain Service
	repo := postgresAdapter.NewPostgresTaskRepository(db)
	loanService := service.NewLoanService(repo, vkAdapter, vkAdapter)

	// 5. Initialize Inbound Delivery Adapter (gRPC Handler)
	grpcHandler := grpcAdapter.NewLoanHandler(loanService, loanService)

	// 6. Create and Start gRPC Server
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

	// 7. Setup gRPC-Gateway HTTP Reverse Proxy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwMux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	grpcEndpoint := "127.0.0.1" + defaultGRPCPort
	if err := loanflowv1.RegisterLoanDocumentProcessorServiceHandlerFromEndpoint(ctx, gwMux, grpcEndpoint, opts); err != nil {
		slog.Error("failed to register gRPC-Gateway handler endpoint", "error", err)
		os.Exit(1)
	}

	// 8. Setup HTTP Router (gRPC-Gateway + OpenAPI Docs)
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
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		slog.Info("Starting LoanFlow Scheduler HTTP Gateway & Swagger Server", "address", defaultHTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP gateway server stopped unexpectedly", "error", err)
		}
	}()

	// 9. Graceful Shutdown Signal Handling
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
