package core

import (
	"github.com/google/uuid"
	"time"
)

type Pubsy interface {
	Topic(name string) Topic
}

type Topic interface {
	Publish(data []byte) error
	Subscribe(consumer string, handle MessageHandler) (Subscription, error)
}

type TopicRepository interface {
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

type SubscriptionRepository interface {
	InsertSubscriptionInfo(info *SubscriptionInfo) error
	GetSubscriptionInfo(ID uuid.UUID) (*SubscriptionInfo, error)
	FindSubscriptionInfoByTopicAndConsumer(topic string, consumer string) (*SubscriptionInfo, error)
	UpdateSubscriptionConsumedOffset(ID uuid.UUID, offset uint64) error
	InsertSubscriptionMessage(message *SubscriptionMessage) error
	ConsumeMessagesFromConsumedOffsetByStatus(topic string, consumer string, status SubscriptionStatus, offset uint64, limit uint64) (*[]SubscriptionMessage, error)
	UpdateSubscriptionMessageStatus(topic string, id uint64, status SubscriptionStatus) error
}

type TopicService interface {
	GetMessagesFromOffset(topic string, offset uint64, limit uint64) (*[]TopicMessage, error)
	GetMessage(topic string, id uint64) (*TopicMessage, error)
	DeleteMessagesOlderThan(topic string, id uint64, duration time.Duration) (*TopicMessage, error)
	Save(topic string, data []byte) (*TopicMessage, error)
}

type SubscriptionService interface {
	GetMessagesFromConsumedOffsetByStatus(topic string, consumer string, status SubscriptionStatus, consumedOffset uint64, limit uint64) (*[]SubscriptionMessage, error)
	RegisterConsumer(topic string, consumer string) Subscription
}
