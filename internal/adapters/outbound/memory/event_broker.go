package memory

import (
	"context"
	"sync"

	"github.com/smashraid/pandora/internal/domain"
	"github.com/smashraid/pandora/internal/ports/outbound"
)

var (
	_ outbound.EventPublisher  = (*MemoryEventBroker)(nil)
	_ outbound.EventSubscriber = (*MemoryEventBroker)(nil)
)

type MemoryEventBroker struct {
	mu          sync.RWMutex
	subscribers map[string][]chan *domain.ProcessingTask
}

func NewMemoryEventBroker() *MemoryEventBroker {
	return &MemoryEventBroker{
		subscribers: make(map[string][]chan *domain.ProcessingTask),
	}
}

func (b *MemoryEventBroker) PublishProgressEvent(ctx context.Context, task *domain.ProcessingTask) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	chans, exists := b.subscribers[task.TaskID]
	if !exists {
		return nil
	}

	taskCopy := *task
	for _, ch := range chans {
		select {
		case ch <- &taskCopy:
		default:
			// Non-blocking write to prevent slow listeners from jamming the pipeline
		}
	}
	return nil
}

func (b *MemoryEventBroker) SubscribeProgressEvents(ctx context.Context, taskID string) (<-chan *domain.ProcessingTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan *domain.ProcessingTask, 100)
	b.subscribers[taskID] = append(b.subscribers[taskID], ch)

	// Clean up subscription channel when context is cancelled
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		defer b.mu.Unlock()

		subs := b.subscribers[taskID]
		for i, sub := range subs {
			if sub == ch {
				b.subscribers[taskID] = append(subs[:i], subs[i+1:]...)
				close(ch)
				break
			}
		}
		if len(b.subscribers[taskID]) == 0 {
			delete(b.subscribers, taskID)
		}
	}()

	return ch, nil
}
