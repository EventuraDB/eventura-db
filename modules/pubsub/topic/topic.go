package topic

import (
	"eventura/modules/pubsub/messages"
	"eventura/modules/pubsub/subscription"
	"eventura/modules/pubsub/types"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"sync"
)

type Topic struct {
	Name          string
	db            *pebble.DB
	log           *zap.Logger
	subscriptions []*subscription.SubscriptionService
	subWg         *sync.WaitGroup
	msgRepo       *messages.MessageRepository
}

func (t *Topic) Publish(data []byte) error {
	t.log.Info("Publishing message", zap.String("topic", t.Name))
	err := t.msgRepo.InsertMessage(t.Name, data)
	if err != nil {
		t.log.Error("Failed to publish message", zap.Error(err))
		return err
	}

	t.log.Info("Message published", zap.String("topic", t.Name))

	return nil
}

func (t *Topic) Subscribe(consumerID string, handler types.MessageHandler) {
	sub := subscription.New(consumerID, t.Name, handler, t.db, t.log)
	sub.Start()

	t.subscriptions = append(t.subscriptions, sub)
}

func NewTopic(name string, db *pebble.DB, msgRepo *messages.MessageRepository, log *zap.Logger) *Topic {
	return &Topic{
		Name:    name,
		db:      db,
		log:     log,
		subWg:   &sync.WaitGroup{},
		msgRepo: msgRepo,
	}
}
