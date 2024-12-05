package pubsy

import (
	"time"
)

type Broker interface {
	Topic(name string) *Topic
}

type Topic interface {
	Publish(data []byte) error
	Subscribe(consumer string, handle MessageHandler) (Subscription, error)
}

type TopicMessageRepository interface {
	Save(topic string, data []byte) (*TopicMessage, error)
	GetMessagesFromOffset(topic string, offset uint64, limit uint64) (*[]TopicMessage, error)
	GetMessage(topic string, id uint64) (*TopicMessage, error)
	DeleteMessagesOlderThan(topic string, duration time.Duration) (uint64, error)
	GetLastMessageID(topic string) (uint64, error)
}

type Subscription interface {
	Start() error
	Stop() error
}

type SubscriptionMessageRepository interface {
	Save(message *SubscriptionMessage) error
	GetMessagesFromOffsetByStatus(topic string, consumer string, status SubscriptionStatus, offset uint64, limit uint64) (*[]SubscriptionMessage, error)
}

type TopicService interface {
	GetMessagesFromOffset(topic string, offset uint64, limit uint64) (*[]TopicMessage, error)
	GetMessage(topic string, id uint64) (*TopicMessage, error)
	DeleteMessagesOlderThan(topic string, id uint64, duration time.Duration) (*TopicMessage, error)
	Save(topic string, data []byte) (*TopicMessage, error)
}

type SubscriptionService interface {
	GetMessagesFromOffsetByStatus(topic string, consumer string, status SubscriptionStatus, offset uint64, limit uint64) (*[]SubscriptionMessage, error)
	RegisterConsumer(topic string, consumer string) Subscription
}
