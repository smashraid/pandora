package valkey

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
	"github.com/smashraid/pandora/pkg/config"
)

var (
	_ outbound.EventPublisher  = (*ValkeyAdapter)(nil)
	_ outbound.EventSubscriber = (*ValkeyAdapter)(nil)
	_ outbound.TaskQueue       = (*ValkeyAdapter)(nil)
)

type ValkeyAdapter struct {
	client *redis.Client
}

func NewValkeyAdapter(cfg config.ValkeyConfig) (*ValkeyAdapter, error) {
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to valkey: %w", err)
	}

	return &ValkeyAdapter{client: client}, nil
}

func (v *ValkeyAdapter) EnqueueTask(ctx context.Context, taskID string, priority domain.Priority) error {
	queueKey := "queue:standard"
	if priority == domain.PriorityHigh {
		queueKey = "queue:high"
	}

	return v.client.RPush(ctx, queueKey, taskID).Err()
}

func (v *ValkeyAdapter) DequeueTask(ctx context.Context, priority domain.Priority) (string, error) {
	queueKey := "queue:standard"
	if priority == domain.PriorityHigh {
		queueKey = "queue:high"
	}

	// BLPop blocks until an item is available or context expires
	// We pass 1 second timeout to allow checking ctx.Done() in worker loops
	res, err := v.client.BLPop(ctx, 1*time.Second, queueKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil // Queue empty timeout, return empty string cleanly
		}
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

	// Ensure the subscription is active before starting the background worker
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("failed to subscribe to progress channel %s: %w", channel, err)
	}

	ch := make(chan *domain.ProcessingTask, 100)

	go func() {
		defer func() {
			_ = pubsub.Close()
		}()
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
