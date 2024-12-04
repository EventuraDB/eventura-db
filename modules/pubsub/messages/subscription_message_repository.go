package messages

import (
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

const (
	SUB_MESSAGES_NAMESPACE = "sub_messages"
)

type SubscriptionMessageRepository struct {
	db  *pebble.DB
	log *zap.Logger
}

type SubscriptionMessage struct {
	ID         uint64 //Pointer to message
	ConsumerID string
	Topic      string
	RetryCount int
	Status     string
}

type SubscriptionMessageDBKey struct {
	Topic string
	ID    uint64
}

func (s *SubscriptionMessageDBKey) Key() []byte {
	return []byte(fmt.Sprintf("%s:%s:%d", SUB_MESSAGES_NAMESPACE, s.Topic, utils.UintToBytes(s.ID)))
}

type SubscriptionMessageDBValue struct {
	MessageID []byte
}

func (v SubscriptionMessageDBValue) Value() []byte {
	return v.MessageID
}

func (r *SubscriptionMessageRepository) SaveSubscriptionMessage(subscriptionMessage SubscriptionMessage) error {
	// For now we assume the ID is always incremental unique number
	dbKey := SubscriptionMessageDBKey{Topic: subscriptionMessage.Topic, ID: subscriptionMessage.ID}
	dbValue := SubscriptionMessageDBValue{MessageID: utils.UintToBytes(subscriptionMessage.ID)}

	err := r.db.Set(dbKey.Key(), dbValue.Value(), pebble.Sync)
	if err != nil {
		r.log.Error("Failed to save subscription message", zap.Error(err))
		return fmt.Errorf("failed to save subscription message: %w", err)
	}

	return nil
}

func (r *SubscriptionMessageRepository) GetSubscriptionMessagesForTopic(topic string) (*[]SubscriptionMessage, error) {
	return nil, nil
}

func (r *SubscriptionMessageRepository) GetSubscriptionMessagesForTopicAndStatus(topic string, status string) (*[]SubscriptionMessage, error) {
	return nil, nil
}
