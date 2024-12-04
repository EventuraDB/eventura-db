package subscription

import (
	"eventura/modules/pubsub/messages"
	"eventura/modules/pubsub/types"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"sync"
	"time"
)

type SubscriptionService struct {
	ID          string
	Topic       string
	ConsumeFunc types.MessageHandler
	stopCh      chan struct{}
	wg          *sync.WaitGroup
	db          *pebble.DB
	log         *zap.Logger
	msgRepo     *messages.MessageRepository
	msgSubRepo  *messages.SubscriptionMessageRepository
	subRepo     *SubscriptionRepository
	fetchMu     *sync.Mutex
	offsetMu    *sync.Mutex
}

type SubscriptionMessage struct {
	ID             uint64
	SubscriptionID string
	State          string //todo make enum
	RetryCount     int
	RetryAt        int64
	Errors         []error
}

type SubscriptionOptions struct {
	RetryPolicy int
	Backoff     int
}

// Close gracefully stops the subscription by closing the stop channel.
func (sub *SubscriptionService) Close() {
	close(sub.stopCh)
}

// Start subscriptions in goroutine
func (sub *SubscriptionService) Start() {
	go func() {
		err := sub.schedule()
		if err != nil {
			sub.log.Error("Failed to notify subscriptions", zap.Error(err))
		}
	}()
}

// schedule for subscription to process messages
func (sub *SubscriptionService) schedule() error {
	for {
		sub.log.Debug("Fetching messages")
		sub.fetchMu.Lock()

		sub.FindOrCreateSubscription(sub.ID, sub.Topic)

		// Get the current offset for the subscription
		subRec, err := sub.subRepo.Read(sub.ID, sub.Topic)
		if err != nil {
			sub.fetchMu.Unlock()
			sub.log.Error("Failed to find subscription record", zap.Error(err))
			time.Sleep(1 * time.Second) // Avoid tight loops
			continue
		}

		// Fetch messages starting from the current offset + 1
		messages, err := sub.msgRepo.GetMessagesInTopicFromOffset(sub.Topic, subRec.Offset+1, 10)
		if err != nil {
			sub.fetchMu.Unlock()
			sub.log.Error("Failed to fetch messages", zap.Error(err))
			time.Sleep(1 * time.Second) // Avoid tight loops
			continue
		}

		if len(messages) == 0 {
			sub.fetchMu.Unlock()
			sub.log.Debug("No new messages found")
			time.Sleep(1 * time.Second) // Avoid tight loops
			continue
		}

		for _, message := range messages {
			sub.log.Debug("Processing message", zap.Uint64("message_id", message.ID))

			err = sub.msgSubRepo.PushMessage(message, sub.ID)
			if err != nil {
				sub.log.Error("Failed to push message", zap.Error(err))
				continue
			}
			// Update the offset after successful processing
			sub.updateOffset(message.ID)
		}

		sub.fetchMu.Unlock()
	}
}

// ShouldStop checks if the subscription has been signaled to stop.
func (sub *SubscriptionService) ShouldStop() bool {
	select {
	case <-sub.stopCh:
		return true
	default:
		return false
	}
}

func (sub *SubscriptionService) updateOffset(offset uint64) {
	sub.offsetMu.Lock()
	defer sub.offsetMu.Unlock()

	err := sub.subRepo.UpdateOffset(sub.ID, sub.Topic, offset)
	if err != nil {
		sub.log.Error("Failed to update offset", zap.Error(err))
		return
	}
	sub.log.Info("Offset updated", zap.Uint64("offset", offset))
}

func (sub *SubscriptionService) readCurrentOffset() uint64 {
	m, _ := sub.subRepo.Read(sub.ID, sub.Topic)
	return m.Offset
}

func (sub *SubscriptionService) calculateOffsetKey() []byte {
	return []byte(fmt.Sprintf("subscription:%s:topic:%s", sub.ID, sub.Topic))
}

func (sub *SubscriptionService) FindOrCreateSubscription(id string, topic string) (*SubscriptionRecord, error) {
	if !sub.subRepo.Exists(id, topic) {
		subRec, err := sub.subRepo.Insert(id, topic)
		if err != nil {
			sub.log.Error("Failed to create subscription", zap.Error(err))
			return nil, fmt.Errorf("failed to create subscription %w", err)
		}
		sub.log.Info("SubscriptionService created", zap.String("id", id), zap.String("topic", topic))
		return subRec, nil
	}
	subRec, err := sub.subRepo.Read(id, topic)
	if err != nil {
		sub.log.Error("Failed to find subscription", zap.Error(err))
		return nil, fmt.Errorf("failed to find subscription %w", err)
	}
	return subRec, nil

}

func New(id string, topicName string, handler types.MessageHandler, db *pebble.DB, log *zap.Logger) *SubscriptionService {

	return &SubscriptionService{
		ID:          id,
		Topic:       topicName,
		ConsumeFunc: handler,
		stopCh:      make(chan struct{}),
		wg:          &sync.WaitGroup{},
		db:          db,
		log:         log,
		msgRepo:     messages.NewMessageRepository(db, log),
		msgSubRepo:  messages.NewSubscriptionMessageRepository(db, log),
		subRepo:     NewSubscriptionRepository(db, log),
		fetchMu:     &sync.Mutex{},
		offsetMu:    &sync.Mutex{},
	}

}
