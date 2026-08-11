package valkey_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smashraid/pandora/internal/adapters/outbound/valkey"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/pkg/config"
)

func setupValkeyAdapter(t *testing.T) *valkey.ValkeyAdapter {
	t.Helper()

	addr := os.Getenv("TEST_VALKEY_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cfg := config.ValkeyConfig{
		Addr:         addr,
		Username:     "admin",
		Password:     "admin",
		DB:           0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     5,
	}

	// Helper client to test connectivity and flush DB before running tests
	flushClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx := context.Background()
	if err := flushClient.Ping(ctx).Err(); err != nil {
		_ = flushClient.Close()
		t.Skipf("skipping test: Valkey instance not reachable at %s: %v", addr, err)
	}

	_ = flushClient.FlushDB(ctx).Err()
	_ = flushClient.Close()

	adapter, err := valkey.NewValkeyAdapter(cfg)
	if err != nil {
		t.Fatalf("failed to initialize Valkey adapter: %v", err)
	}

	return adapter
}

func TestValkeyAdapter_PriorityQueueing(t *testing.T) {
	adapter := setupValkeyAdapter(t)
	ctx := context.Background()

	// Enqueue standard and high priority tasks
	_ = adapter.EnqueueTask(ctx, "standard-task-1", domain.PriorityStandard)
	_ = adapter.EnqueueTask(ctx, "high-task-1", domain.PriorityHigh)

	// Dequeue high priority task
	dequeuedFirst, err := adapter.DequeueTask(ctx, domain.PriorityHigh)
	if err != nil {
		t.Fatalf("failed to dequeue high-priority item: %v", err)
	}
	if dequeuedFirst != "high-task-1" {
		t.Errorf("expected high-priority task first, got: %s", dequeuedFirst)
	}

	// Dequeue standard priority task
	dequeuedSecond, err := adapter.DequeueTask(ctx, domain.PriorityStandard)
	if err != nil {
		t.Fatalf("failed to dequeue standard-priority item: %v", err)
	}
	if dequeuedSecond != "standard-task-1" {
		t.Errorf("expected standard-priority task second, got: %s", dequeuedSecond)
	}
}

func TestValkeyAdapter_PubSub(t *testing.T) {
	adapter := setupValkeyAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	taskID := "valkey-pubsub-001"
	ch, err := adapter.SubscribeProgressEvents(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	event := &domain.ProcessingTask{
		TaskID:             taskID,
		ApplicationID:      "app-001",
		Status:             domain.TaskStatusProcessing,
		CurrentStage:       domain.StageCreditBureauCheck,
		ProgressPercentage: 65,
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = adapter.PublishProgressEvent(ctx, event)
	}()

	select {
	case received := <-ch:
		if received.TaskID != taskID {
			t.Errorf("expected task %s, got %s", taskID, received.TaskID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for pubsub event")
	}
}
