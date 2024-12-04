package messages

import (
	"encoding/json"
	"eventura/modules/pubsub/types"
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

type SearchByIDKey struct {
	ID uint64
}

func (s *SubscriptionMessageDBKey) Key() []byte {
	return []byte(fmt.Sprintf("%s:%s:%d", SUB_MESSAGES_NAMESPACE, s.Topic, utils.UintToBytes(s.ID)))
}

type SubscriptionMessageDBValue struct {
	value *SubscriptionMessage
}

func (v SubscriptionMessageDBValue) Value() ([]byte, error) {
	data, err := json.Marshal(v.value)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *SubscriptionMessageRepository) PushMessage(msg types.Message, consumerID string) error {
	// For now we assume the ID is always incremental unique number
	dbKey := SubscriptionMessageDBKey{Topic: msg.Topic, ID: msg.ID}
	dbValue := SubscriptionMessageDBValue{value: &SubscriptionMessage{
		ID:         msg.ID,
		ConsumerID: consumerID,
		Topic:      msg.Topic,
		RetryCount: 0,
		Status:     "new",
	},
	}

	data, err := dbValue.Value()
	if err != nil {
		r.log.Error("Failed to save subscription message", zap.Error(err))
		return fmt.Errorf("failed to save subscription message: %w", err)
	}

	err = r.db.Set(dbKey.Key(), data, pebble.Sync)
	if err != nil {
		r.log.Error("Failed to save subscription message", zap.Error(err))
		return fmt.Errorf("failed to save subscription message: %w", err)
	}

	r.log.Info("Message pushed", zap.Uint64("id", msg.ID), zap.String("topic", msg.Topic))

	return nil
}

func (r *SubscriptionMessageRepository) SaveSubscriptionMessage(subscriptionMessage SubscriptionMessage) error {
	// For now we assume the ID is always incremental unique number
	dbKey := SubscriptionMessageDBKey{Topic: subscriptionMessage.Topic, ID: subscriptionMessage.ID}
	dbValue := SubscriptionMessageDBValue{value: &subscriptionMessage}

	data, err := dbValue.Value()
	if err != nil {
		r.log.Error("Failed to save subscription message", zap.Error(err))
		return fmt.Errorf("failed to save subscription message: %w", err)
	}

	err = r.db.Set(dbKey.Key(), data, pebble.Sync)
	if err != nil {
		r.log.Error("Failed to save subscription message", zap.Error(err))
		return fmt.Errorf("failed to save subscription message: %w", err)
	}

	return nil
}

// Get Message by ID and Topic
func (r *SubscriptionMessageRepository) FindMessage(id uint64, topic string) (*SubscriptionMessage, error) {
	key := SubscriptionMessageDBKey{ID: id, Topic: topic}
	data, closer, err := r.db.Get(key.Key())
	if err != nil {
		r.log.Error("failed to get message", zap.Error(err))
		return nil, fmt.Errorf("failed to get message: %w", err)
	}
	defer closer.Close()

	var value SubscriptionMessage
	err = json.Unmarshal(data, &value)
	if err != nil {
		r.log.Error("failed to unmarshal message", zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return &value, nil
}

func (r *SubscriptionMessageRepository) GetSubscriptionMessagesForTopic(topic string) (*[]SubscriptionMessage, error) {
	dbKeyLower := SubscriptionMessageDBKey{Topic: topic, ID: 0}
	dbKeyUpper := SubscriptionMessageDBKey{Topic: topic, ID: ^uint64(0)}

	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: dbKeyLower.Key(), UpperBound: dbKeyUpper.Key()})
	defer iter.Close()
	if err != nil {
		r.log.Error("failed to get messages", zap.Error(err))
		return nil, fmt.Errorf("failed to get messages: %w", err)
	}

	messages := make([]SubscriptionMessage, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		var value SubscriptionMessage
		err := json.Unmarshal(iter.Value(), &value)
		if err != nil {
			r.log.Error("failed to unmarshal message", zap.Error(err))
			return nil, fmt.Errorf("failed to unmarshal message: %w", err)
		}

		messages = append(messages, value)
	}

	return &messages, nil

}

func (r *SubscriptionMessageRepository) GetSubscriptionMessagesForTopicAndStatus(topic string, status string) (*[]SubscriptionMessage, error) {

	messages, err := r.GetSubscriptionMessagesForTopic(topic) //TODO seems really inefficient to load all messages in topic
	if err != nil {
		r.log.Error("failed to get messages", zap.Error(err))
		return nil, fmt.Errorf("failed to get messages: %w", err)
	}
	var filteredMessages []SubscriptionMessage
	for _, message := range *messages {
		if message.Status == status {
			filteredMessages = append(filteredMessages, message)
		}
	}

	return &filteredMessages, nil
}

func (r *SubscriptionMessageRepository) UpdateMessageStatus(id uint64, topic string, status string) (*SubscriptionMessage, error) {
	msg, err := r.FindMessage(id, topic)
	if err != nil {
		r.log.Error("failed to get message", zap.Error(err))
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	msg.Status = status

	dbKey := SubscriptionMessageDBKey{ID: id, Topic: topic}
	dbValue := SubscriptionMessageDBValue{value: msg}
	data, err := dbValue.Value()
	if err != nil {
		r.log.Error("Failed to update message status", zap.Error(err))
		return nil, fmt.Errorf("failed to update message status: %w", err)
	}

	r.db.Set(dbKey.Key(), data, pebble.Sync)

	return msg, nil
}

func NewSubscriptionMessageRepository(db *pebble.DB, log *zap.Logger) *SubscriptionMessageRepository {
	return &SubscriptionMessageRepository{
		db:  db,
		log: log,
	}
}
