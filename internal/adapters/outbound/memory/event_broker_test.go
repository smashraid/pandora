package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/smashraid/pandora/internal/adapters/outbound/memory"
	"github.com/smashraid/pandora/internal/domain"
)

func TestMemoryEventBroker_PublishAndSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	broker := memory.NewMemoryEventBroker()
	taskID := "task-stream-777"

	// 1. Subscribe to events
	ch, err := broker.SubscribeProgressEvents(ctx, taskID)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	event := &domain.ProcessingTask{
		TaskID:             taskID,
		ApplicationID:      "app-777",
		Status:             domain.TaskStatusProcessing,
		CurrentStage:       domain.StageCreditBureauCheck,
		ProgressPercentage: 65,
	}

	// 2. Publish event asynchronously
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := broker.PublishProgressEvent(ctx, event); err != nil {
			t.Errorf("failed to publish: %v", err)
		}
	}()

	// 3. Receive event from stream
	select {
	case received, ok := <-ch:
		if !ok {
			t.Fatal("channel was closed unexpectedly")
		}
		if received.TaskID != taskID {
			t.Errorf("expected task ID %s, got %s", taskID, received.TaskID)
		}
		if received.ProgressPercentage != 65 {
			t.Errorf("expected progress percentage 65, got %d", received.ProgressPercentage)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for published event")
	}
}
