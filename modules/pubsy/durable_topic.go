package pubsy

import (
	"go.uber.org/zap"
)

type DurableTopic struct {
	BaseDependency
	Topic         string
	repo          TopicMessageRepository
	subService    SubscriptionService
	subscriptions map[string]Subscription
}

func (t *DurableTopic) Subscribe(consumer string, handle MessageHandler) (Subscription, error) {
	subscription := t.subService.RegisterConsumer(t.Topic, consumer)
	t.subscriptions[consumer] = subscription

	err := subscription.Start()
	if err != nil {
		t.Log.Error("Error starting subscription", zap.String("consumer", consumer), zap.Error(err))
		return nil, err
	}

	return subscription, nil
}

func (t *DurableTopic) Publish(data []byte) error {
	topicMsg, err := t.repo.Save(t.Topic, data)
	if err != nil {
		t.Log.Error("Error publishing to topic", zap.String("topic", t.Topic), zap.Error(err))
	}

	t.Log.Info("Published message to topic", zap.String("topic", t.Topic), zap.Uint64("message_id", topicMsg.ID))

	return nil
}

func NewDurableTopic(topicName string, repo TopicMessageRepository, dependency BaseDependency) Topic {
	topic := &DurableTopic{
		Topic:          topicName,
		repo:           repo,
		BaseDependency: dependency,
	}

	return topic
}
