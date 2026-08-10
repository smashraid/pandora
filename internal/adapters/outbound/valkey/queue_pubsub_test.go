package valkey_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smashraid/pandora/internal/adapters/outbound/valkey"
	"github.com/smashraid/pandora/internal/domain"
)

func setupValkeyClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_VALKEY_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: "admin",
		Password: "admin",
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping test: Valkey instance not reachable at %s: %v", addr, err)
	}

	// Flush test keys for clean test state
	_ = client.FlushDB(ctx).Err()

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func TestValkeyAdapter_PriorityQueueing(t *testing.T) {
	client := setupValkeyClient(t)
	adapter := valkey.NewValkeyAdapter(client)
	ctx := context.Background()

	// Enqueue standard and high priority tasks
	_ = adapter.EnqueueTask(ctx, "standard-task-1", domain.PriorityStandard)
	_ = adapter.EnqueueTask(ctx, "high-task-1", domain.PriorityHigh)

	// High priority task must be popped first
	dequeuedFirst, err := adapter.DequeueTask(ctx)
	if err != nil {
		t.Fatalf("failed to dequeue first item: %v", err)
	}
	if dequeuedFirst != "high-task-1" {
		t.Errorf("expected high-priority task first, got: %s", dequeuedFirst)
	}

	dequeuedSecond, _ := adapter.DequeueTask(ctx)
	if dequeuedSecond != "standard-task-1" {
		t.Errorf("expected standard-priority task second, got: %s", dequeuedSecond)
	}
}

func TestValkeyAdapter_PubSub(t *testing.T) {
	client := setupValkeyClient(t)
	adapter := valkey.NewValkeyAdapter(client)
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
