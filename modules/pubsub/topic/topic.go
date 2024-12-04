package topic

import (
	"eventura/modules/pubsub/subscription"
	"eventura/modules/pubsub/types"
	"eventura/modules/utils"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"sync"
)

type Topic struct {
	Name          string
	db            *pebble.DB
	log           *zap.Logger
	subscriptions []*subscription.Subscription
	subWg         *sync.WaitGroup
}

func (t *Topic) Publish(message types.Message) error {
	seq, _ := utils.NewSequenceGenerator(t.db, "topicx", 10)
	message.ID = seq.Next()
	err := t.db.Set(message.Key(), message.Data, pebble.Sync)
	if err != nil {
		t.log.Error("failed to publish message", zap.Error(err))
		return err
	}
	return nil
}

func (t *Topic) Subscribe(consumerID string, options subscription.SubscriptionOptions, handler types.ConsumerFunc) {
	sub := subscription.New(consumerID, t, options, handler, t.db, t.log)
	sub.Start(t.subWg)

	t.subscriptions = append(t.subscriptions, sub)
}

func NewTopic(name string, db *pebble.DB, log *zap.Logger) *Topic {
	return &Topic{
		Name:  name,
		db:    db,
		log:   log,
		subWg: &sync.WaitGroup{},
	}
}
