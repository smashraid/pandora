package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	memoryAdapter "github.com/smashraid/pandora/internal/adapters/outbound/memory"
	postgresAdapter "github.com/smashraid/pandora/internal/adapters/outbound/postgres"
	valkeyAdapter "github.com/smashraid/pandora/internal/adapters/outbound/valkey"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
	"github.com/smashraid/pandora/internal/service"
	"github.com/smashraid/pandora/pkg/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Initializing LoanFlow Worker Service...")

	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Warn("Failed to load environment config, using defaults", "error", err)
		cfg = &config.Config{}
	}

	// 1. Initialize Database Connection & Task Repository
	var taskRepo outbound.TaskRepository
	var db *sql.DB

	if cfg.Database.DatabaseURL != "" {
		sqlDB, err := sql.Open(cfg.Database.DriverName, cfg.Database.DatabaseURL)
		if err != nil {
			slog.Error("Failed to open database handle, falling back to memory store", "error", err)
			taskRepo = memoryAdapter.NewMemoryTaskRepository()
		} else {
			sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
			sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
			sqlDB.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)

			pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := sqlDB.PingContext(pingCtx); err != nil {
				pingCancel()
				slog.Error("Failed to ping PostgreSQL database, falling back to memory store", "error", err)
				_ = sqlDB.Close()
				taskRepo = memoryAdapter.NewMemoryTaskRepository()
			} else {
				pingCancel()
				db = sqlDB
				taskRepo = postgresAdapter.NewPostgresTaskRepository(db)
				slog.Info("PostgreSQL task repository connected successfully")
			}
		}
	} else {
		slog.Info("No database URL provided, using in-memory task repository")
		taskRepo = memoryAdapter.NewMemoryTaskRepository()
	}

	if db != nil {
		defer func() {
			if err := db.Close(); err != nil {
				slog.Error("Error closing database connection pool", "error", err)
			}
		}()
	}

	// 2. Initialize Queue & PubSub Adapters with Valkey fallback to Memory
	var taskQueue outbound.TaskQueue
	var eventPubSub outbound.EventSubscriber

	vAdapter, err := valkeyAdapter.NewValkeyAdapter(cfg.Valkey)
	if err != nil {
		slog.Error("Failed to connect to Valkey, falling back to memory broker", "error", err)
		memBroker := memoryAdapter.NewMemoryEventBroker()
		taskQueue = memBroker
		eventPubSub = memBroker
	} else {
		taskQueue = vAdapter
		eventPubSub = vAdapter
		slog.Info("Valkey task queue & pubsub connected successfully")
	}

	// 3. Initialize Worker Processor
	processor := service.NewWorkerProcessor(taskRepo, taskQueue, eventPubSub, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	// Dedicated high-priority workers
	highPriorityWorkers := 3
	for i := 1; i <= highPriorityWorkers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			processor.StartWorker(ctx, workerID, domain.PriorityHigh)
		}()
	}

	// Dedicated standard-priority workers
	standardPriorityWorkers := 2
	for i := 1; i <= standardPriorityWorkers; i++ {
		wg.Add(1)
		workerID := highPriorityWorkers + i
		go func() {
			defer wg.Done()
			processor.StartWorker(ctx, workerID, domain.PriorityStandard)
		}()
	}

	slog.Info("Worker pool initialized",
		"high_priority_workers", highPriorityWorkers,
		"standard_priority_workers", standardPriorityWorkers)

	<-ctx.Done()
	slog.Info("Shutdown signal received. Waiting for workers to terminate...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		slog.Info("All workers terminated cleanly")
	case <-shutdownCtx.Done():
		slog.Warn("Worker shutdown timed out")
	}
}
