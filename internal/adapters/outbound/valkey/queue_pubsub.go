package valkey

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
)

var (
	_ outbound.EventPublisher  = (*ValkeyAdapter)(nil)
	_ outbound.EventSubscriber = (*ValkeyAdapter)(nil)
)

type ValkeyAdapter struct {
	client *redis.Client
}

func NewValkeyAdapter(client *redis.Client) *ValkeyAdapter {
	return &ValkeyAdapter{client: client}
}

func (v *ValkeyAdapter) EnqueueTask(ctx context.Context, taskID string, priority domain.Priority) error {
	queueKey := "queue:standard"
	if priority == domain.PriorityHigh {
		queueKey = "queue:high"
	}

	return v.client.RPush(ctx, queueKey, taskID).Err()
}

func (v *ValkeyAdapter) DequeueTask(ctx context.Context) (string, error) {
	// BLPop checks queue:high first before falling back to queue:standard
	res, err := v.client.BLPop(ctx, 0, "queue:high", "queue:standard").Result()
	if err != nil {
		return "", err
	}
	return res[1], nil
}

func (v *ValkeyAdapter) PublishProgressEvent(ctx context.Context, task *domain.ProcessingTask) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task update: %w", err)
	}

	channel := fmt.Sprintf("task:progress:%s", task.TaskID)
	return v.client.Publish(ctx, channel, payload).Err()
}

func (v *ValkeyAdapter) SubscribeProgressEvents(ctx context.Context, taskID string) (<-chan *domain.ProcessingTask, error) {
	channel := fmt.Sprintf("task:progress:%s", taskID)
	pubsub := v.client.Subscribe(ctx, channel)

	ch := make(chan *domain.ProcessingTask, 100)

	go func() {
		defer pubsub.Close()
		defer close(ch)

		valkeyChan := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-valkeyChan:
				if !ok {
					return
				}
				var task domain.ProcessingTask
				if err := json.Unmarshal([]byte(msg.Payload), &task); err == nil {
					ch <- &task
				}
			}
		}
	}()

	return ch, nil
}
