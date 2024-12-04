package subscription

import (
	"eventura/modules/pubsub/messages"
	"eventura/modules/pubsub/queue"
	"eventura/modules/pubsub/types"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"sync"
)

type Subscription struct {
	ID          string
	Topic       string
	offset      uint64
	ConsumeFunc types.ConsumerFunc
	stopCh      chan struct{}
	offsetKey   []byte
	wg          *sync.WaitGroup
	db          *pebble.DB
	log         *zap.Logger
	queue       *queue.FifoQueue
	msgRepo     *messages.MessageRepository
	msgSubRepo  *messages.SubscriptionMessageRepository
	subRepo     *SubscriptionRepository
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
func (sub *Subscription) Close() {
	close(sub.stopCh)
}

// Start subscrptions in goroutine
func (sub *Subscription) Start() {
	exists := sub.subRepo.Exists(sub.ID, sub.Topic)
	if exists {
		sub.subRepo.Insert(sub.ID, sub.Topic)
	}
	subs, _ := sub.subRepo.Find(sub.ID, sub.Topic)

	sub.msgRepo.GetMessagesInTopic(sub.Topic, 10)

}

func (sub *Subscription) loadData(topic string, offset uint64, limit uint64) []types.Message {
	return nil
}

// ShouldStop checks if the subscription has been signaled to stop.
func (sub *Subscription) ShouldStop() bool {
	select {
	case <-sub.stopCh:
		return true
	default:
		return false
	}
}

func (sub *Subscription) updateOffset(offset uint64) {
	err := sub.db.Set(sub.offsetKey, utils.UintToBytes(offset), pebble.Sync)
	if err != nil {
		sub.log.Error("failed to update offset", zap.Error(err))
		return
	}
}

func (sub *Subscription) readCurrentOffset() uint64 {
	return sub.offset
}

func (sub *Subscription) calculateOffsetKey() []byte {
	return []byte(fmt.Sprintf("subscription:%s:topic:%s", sub.ID, sub.Topic))
}

func (sub *Subscription) Subscribe() {
	//
}
