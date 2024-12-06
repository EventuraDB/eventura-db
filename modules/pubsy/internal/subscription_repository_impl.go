package internal

import (
	"encoding/json"
	"eventura/modules/pubsy/core"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"math"
)

const (
	SUB_INFO_KEYSPACE = "sub_info"
	SUB_MSG_KEYSPACE  = "sub_message"
)

type SubscriptionRepositoryImpl struct {
	log *zap.Logger
	db  *pebble.DB
}

func (s *SubscriptionRepositoryImpl) UpdateSubscriptionMessageStatus(topic string, id uint64, status core.SubscriptionStatus) error {
	key := utils.NewCompositeKey(SUB_MSG_KEYSPACE).
		AddString(topic).
		AddUint64(id).Build()

	data, closer, err := s.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			closer.Close()
			s.log.Error("Message not found", zap.Error(err))
			return fmt.Errorf("message not found: %w", err)
		}
		closer.Close()
		s.log.Error("Error getting message", zap.Error(err))
		return fmt.Errorf("error getting message: %w", err)
	}
	closer.Close()

	var message core.SubscriptionMessage
	if err = json.Unmarshal(data, &message); err != nil {
		s.log.Error("Error unmarshalling message", zap.Error(err))
		return fmt.Errorf("error unmarshalling message: %w", err)
	}
	message.Status = status

	updateData, err := json.Marshal(message)
	if err != nil {
		s.log.Error("Error marshalling message", zap.Error(err))
		return fmt.Errorf("error marshalling message: %w", err)
	}

	err = s.db.Set(key, updateData, pebble.Sync)
	if err != nil {
		s.log.Error("Error updating message", zap.Error(err))
		return fmt.Errorf("error updating message: %w", err)
	}

	return nil

}

func (s *SubscriptionRepositoryImpl) GetSubscriptionInfo(ID uuid.UUID) (*core.SubscriptionInfo, error) {
	// Define the lower and upper bounds for the prefix scan
	lowerBound := utils.NewCompositeKey(SUB_INFO_KEYSPACE).
		AddString(ID.String()).
		Build()

	upperBound := utils.NewCompositeKey(SUB_INFO_KEYSPACE).
		AddString(ID.String()).
		AddString("\uffff"). // High-value string to ensure we include all keys for this ID
		Build()

	// Create an iterator for the prefix scan
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		s.log.Error("Error creating iterator for GetSubscriptionInfo", zap.Error(err))
		return nil, fmt.Errorf("error creating iterator: %w", err)
	}
	defer iter.Close()

	// Look for the first matching record
	if iter.First() && iter.Valid() {
		data, err := iter.ValueAndErr()
		if err != nil {
			s.log.Error("Error reading subscription info value", zap.Error(err))
			return nil, fmt.Errorf("error reading subscription info value: %w", err)
		}

		// Unmarshal the data into SubscriptionInfo
		var info core.SubscriptionInfo
		if err := json.Unmarshal(data, &info); err != nil {
			s.log.Error("Error unmarshalling subscription info", zap.Error(err))
			return nil, fmt.Errorf("error unmarshalling subscription info: %w", err)
		}

		return &info, nil
	}

	// No matching subscription info found
	return nil, nil
}

func (s *SubscriptionRepositoryImpl) FindSubscriptionInfoByTopicAndConsumer(topic string, consumer string) (*core.SubscriptionInfo, error) {
	lowerBound := utils.NewCompositeKey(SUB_INFO_KEYSPACE).
		AddString("00000000-0000-0000-0000-000000000000"). // Minimum UUID value
		AddString(topic).
		AddString(consumer).
		Build()

	upperBound := utils.NewCompositeKey(SUB_INFO_KEYSPACE).
		AddString(uuid.Max.String()).
		AddString(topic).
		AddString(consumer).
		Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		s.log.Error("Error creating iterator for FindSubscriptionInfoByTopicAndConsumer", zap.Error(err))
		return nil, fmt.Errorf("error creating iterator: %w", err)
	}
	defer iter.Close()

	// Scan for a key that includes topic and consumer in the suffix
	for iter.First(); iter.Valid(); iter.Next() {
		// Parse the key and ensure it matches the expected topic and consumer
		compositeKey := utils.NewCompositeKey(SUB_INFO_KEYSPACE)
		if err := compositeKey.Parse(iter.Key()); err != nil {
			s.log.Error("Error parsing key", zap.Error(err))
			continue
		}

		uuidField, _ := compositeKey.GetString() // UUID field
		topicField, _ := compositeKey.GetString()
		consumerField, _ := compositeKey.GetString()

		if topicField == topic && consumerField == consumer {
			data, err := iter.ValueAndErr()
			if err != nil {
				s.log.Error("Error reading subscription info value", zap.Error(err))
				return nil, fmt.Errorf("error reading subscription info value: %w", err)
			}

			var info core.SubscriptionInfo
			if err := json.Unmarshal(data, &info); err != nil {
				s.log.Error("Error unmarshalling subscription info", zap.Error(err))
				return nil, fmt.Errorf("error unmarshalling subscription info: %w", err)
			}

			s.log.Info("Found matching subscription info", zap.String("UUID", uuidField))
			return &info, nil
		}
	}

	s.log.Warn("No matching subscription info found", zap.String("topic", topic), zap.String("consumer", consumer))
	return nil, nil
}

func (s *SubscriptionRepositoryImpl) InsertSubscriptionInfo(info *core.SubscriptionInfo) error {
	info.ID = uuid.New()
	key := utils.NewCompositeKey(SUB_INFO_KEYSPACE).
		AddString(info.ID.String()).
		AddString(info.Topic).
		AddString(info.Consumer).
		Build()

	data, err := json.Marshal(info)
	if err != nil {
		s.log.Error("Error marshalling subscription info", zap.Error(err))
		return fmt.Errorf("error marshalling subscription info: %w", err)
	}
	err = s.db.Set(key, data, pebble.Sync)
	if err != nil {
		s.log.Error("Error saving subscription info", zap.Error(err))
		return fmt.Errorf("error saving subscription info: %w", err)
	}

	return nil

}

func (s *SubscriptionRepositoryImpl) UpdateSubscriptionInfo(info *core.SubscriptionInfo) error {
	if info.ID == uuid.Nil {
		s.log.Error("Subscription info ID must not be nil")
		return fmt.Errorf("subscription info ID must not be nil")
	}
	key := utils.NewCompositeKey(SUB_INFO_KEYSPACE).
		AddString(info.ID.String()).
		AddString(info.Topic).
		AddString(info.Consumer).
		Build()

	data, err := json.Marshal(info)
	if err != nil {
		s.log.Error("Error marshalling subscription info", zap.Error(err))
		return fmt.Errorf("error marshalling subscription info: %w", err)
	}
	err = s.db.Set(key, data, pebble.Sync)
	if err != nil {
		s.log.Error("Error saving subscription info", zap.Error(err))
		return fmt.Errorf("error saving subscription info: %w", err)
	}

	return nil

}

func (s *SubscriptionRepositoryImpl) UpdateSubscriptionConsumedOffset(ID uuid.UUID, offset uint64) error {
	subInfo, err := s.GetSubscriptionInfo(ID)
	if err != nil {
		s.log.Error("Error getting subscription info", zap.Error(err))
		return fmt.Errorf("error getting subscription info: %w", err)
	}
	subInfo.LastConsumedOffset = offset

	if err = s.UpdateSubscriptionInfo(subInfo); err != nil {
		s.log.Error("Error updating subscription info", zap.Error(err))
		return fmt.Errorf("error updating subscription info: %w", err)
	}

	return nil

}

func (s *SubscriptionRepositoryImpl) InsertSubscriptionMessage(message *core.SubscriptionMessage) error {
	key := utils.NewCompositeKey(SUB_MSG_KEYSPACE).
		AddString(message.Topic).
		AddUint64(message.Pointer).Build()

	data, err := json.Marshal(message)
	if err != nil {
		s.log.Error("Error marshalling message", zap.Error(err))
		return fmt.Errorf("error marshalling message: %w", err)
	}

	if err = s.db.Set(key, data, pebble.Sync); err != nil {
		s.log.Error("Error saving message", zap.Error(err))
		return fmt.Errorf("error saving message: %w", err)
	}

	return nil

}

// Read messages and immidiately mark them as consumed
func (s *SubscriptionRepositoryImpl) ConsumeMessagesFromConsumedOffsetByStatus(topic string, consumer string, status core.SubscriptionStatus, offset uint64, limit uint64) (*[]core.SubscriptionMessage, error) {
	lowerBound := utils.NewCompositeKey(SUB_MSG_KEYSPACE).AddString(topic).AddUint64(offset).Build()
	upperBound := utils.NewCompositeKey(SUB_MSG_KEYSPACE).AddString(topic).AddUint64(math.MaxUint64).Build()

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	if err != nil {
		s.log.Error("Error creating iterator", zap.Error(err))
		return nil, fmt.Errorf("error creating iterator: %w", err)
	}
	defer utils.HandleAndLog(iter.Close, s.log)

	messages := make([]core.SubscriptionMessage, 0)
	for iter.First(); iter.Valid() && uint64(len(messages)) <= limit; iter.Next() {
		data, err := iter.ValueAndErr()
		if err != nil {
			s.log.Error("Error getting message value", zap.Error(err))
			return nil, fmt.Errorf("error getting message value: %w", err)
		}

		var message core.SubscriptionMessage
		if err = json.Unmarshal(data, &message); err != nil {
			s.log.Error("Error unmarshalling message", zap.Error(err))
			return nil, fmt.Errorf("error unmarshalling message: %w", err)
		}

		messages = append(messages, message)
		offset = message.Pointer //Update offset
	}

	return &messages, nil
}

func NewSubscriptionRepositoryImpl(db *pebble.DB, log *zap.Logger) *SubscriptionRepositoryImpl {
	return &SubscriptionRepositoryImpl{log: log, db: db}
}
