package internal

import (
	"eventura/modules/pubsy/core"
	"go.uber.org/zap"
)

type DurableTopic struct {
	Topic         string
	subscriptions map[string]core.Subscription
	log           *zap.Logger
	repos         *Repos
}

func (t *DurableTopic) Subscribe(consumer string, handle core.MessageHandler) (core.Subscription, error) {

	subscription := NewPullSubscription(t.Topic, consumer, handle, t.repos, t.log)

	t.subscriptions[consumer] = subscription

	err := subscription.Start()
	if err != nil {
		t.log.Error("Error starting subscription", zap.String("consumer", consumer), zap.Error(err))
		return nil, err
	}

	return subscription, nil
}

func (t *DurableTopic) Publish(data []byte) error {
	topicMsg, err := t.repos.TopicRepo.Save(t.Topic, data)
	if err != nil {
		t.log.Error("Error publishing to topic", zap.String("topic", t.Topic), zap.Error(err))
	}

	t.log.Info("Published message to topic", zap.String("topic", t.Topic), zap.Uint64("message_id", topicMsg.ID))

	return nil
}

func NewDurableTopic(topicName string, repos *Repos, logger *zap.Logger) core.Topic {
	topic := &DurableTopic{
		Topic:         topicName,
		log:           logger,
		repos:         repos,
		subscriptions: make(map[string]core.Subscription),
	}

	return topic
}
