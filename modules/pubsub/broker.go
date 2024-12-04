package pubsub

import (
	"eventura/modules/pubsub/messages"
	"eventura/modules/pubsub/subscription"
	"eventura/modules/pubsub/topic"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"log"
	"sync"
)

type Broker struct {
	Db        *pebble.DB
	log       *zap.Logger
	consumers map[string]*subscription.SubscriptionService
	wg        sync.WaitGroup
	msgRepo   *messages.MessageRepository
}

func NewBroker(path string) *Broker {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		log.Fatalf("failed to open broker Db: %v", err)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to create broker logger: %v", err)
	}

	return &Broker{
		Db:        db,
		log:       logger,
		consumers: make(map[string]*subscription.SubscriptionService),
		msgRepo:   messages.NewMessageRepository(db, logger),
	}
}

func (b *Broker) Close() error {
	b.log.Info("Stopping all consumers")
	for _, consumer := range b.consumers {
		consumer.Close()
	}
	b.wg.Wait()
	b.consumers = make(map[string]*subscription.SubscriptionService) // Clear the map

	b.log.Info("Closing broker")
	if err := b.Db.Close(); err != nil {
		b.log.Error("Failed to close database", zap.Error(err))
		return err
	}
	return nil
}

func (b *Broker) NewTopic(s string) *topic.Topic {
	return topic.NewTopic(s, b.Db, b.msgRepo, b.log) //TODO also here functional options ?
}
