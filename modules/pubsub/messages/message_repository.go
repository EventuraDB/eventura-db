package messages

import (
	"bytes"
	"eventura/modules/pubsub/types"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"math"
)

const (
	MESSAGES_NAMESPACE = "messages"
)

type MessageRepository struct {
	db  *pebble.DB
	log *zap.Logger
	seq *utils.SequenceGenerator
}

type MessageDBKey struct {
	Topic string
	ID    uint64
}

func (m *MessageDBKey) Key() []byte {
	return append([]byte(fmt.Sprintf("%s:%s:", MESSAGES_NAMESPACE, m.Topic)), utils.UintToBytes(m.ID)...)
}

type MessageDBValue struct {
	Data []byte `json:"data"`
}

func (v MessageDBValue) Value() []byte {
	return v.Data
}

func NewMessageRepository(db *pebble.DB, log *zap.Logger) *MessageRepository {
	seq, _ := utils.NewSequenceGenerator(db, MESSAGES_NAMESPACE, 10)
	return &MessageRepository{
		db:  db,
		log: log,
		seq: seq,
	}
}

func fromDBKey(key []byte) MessageDBKey {
	parts := bytes.Split(key, []byte(":"))
	topic := string(parts[1])
	id := utils.BytesToUint(parts[2])

	return MessageDBKey{
		Topic: topic,
		ID:    id,
	}
}

// Fixme only message for specific subscription / consumer
func (r *MessageRepository) GetMessagesInTopicFromOffset(topic string, offset uint64, limit uint64) ([]types.Message, error) {
	r.log.Info("Fetching messages", zap.String("topic", topic), zap.Uint64("offset", offset), zap.Uint64("limit", limit))

	lowerBound := MessageDBKey{topic, offset}
	upperBound := MessageDBKey{topic, math.MaxUint64} // Ensure we fetch all messages for the topic

	r.log.Info("Iterator bounds",
		zap.String("lowerBound", string(lowerBound.Key())),
		zap.String("upperBound", string(upperBound.Key())),
	)

	iter, err := r.db.NewIter(
		&pebble.IterOptions{
			LowerBound: lowerBound.Key(),
			UpperBound: upperBound.Key(),
		})
	if err != nil {
		r.log.Error("Failed to create iterator", zap.Error(err))
		return nil, err
	}
	defer utils.HandleAndLog(iter.Close, r.log)

	count := 0
	var messages []types.Message
	for iter.First(); iter.Valid(); iter.Next() {
		dbKey := fromDBKey(iter.Key())
		r.log.Info("Iterating key", zap.ByteString("key", iter.Key()))

		// Skip messages below the requested offset
		if dbKey.ID < offset {
			continue
		}

		// Add message to results
		messages = append(messages, types.Message{
			ID:    dbKey.ID,
			Data:  iter.Value(),
			Topic: dbKey.Topic,
		})

		// Stop when the limit is reached
		count++
		if count >= int(limit) {
			break
		}
	}

	return messages, nil
}

func (r *MessageRepository) GetMessagesInTopic(topic string, limit uint64) ([]types.Message, error) {
	iter, _ := r.db.NewIter(nil)
	defer iter.Close()

	prefix := fmt.Sprintf("%s:%s:", MESSAGES_NAMESPACE, topic)

	count := 0
	var messages []types.Message
	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		count++
		if count >= int(limit) {
			break
		}
		dbKey := fromDBKey(iter.Key())

		messages = append(messages, types.Message{
			ID:    dbKey.ID,
			Data:  iter.Value(),
			Topic: dbKey.Topic,
		})
	}

	return messages, nil
}
func (r *MessageRepository) InsertMessage(topic string, data []byte) error {

	message := types.Message{
		ID:    r.seq.Next(),
		Data:  data,
		Topic: topic,
	}

	dbValue := MessageDBValue{Data: message.Data}

	dbKey := MessageDBKey{message.Topic, message.ID}

	err := r.db.Set(dbKey.Key(), dbValue.Value(), pebble.Sync)

	r.log.Info("Inserting message", zap.String("topic", topic), zap.Uint64("id", message.ID), zap.Any("key", dbKey.Key()))

	if err != nil {
		return err
	}
	return nil
}

//TODO message record vs types.messges

func (r *MessageRepository) GetMessageFromTopic(id uint64, topic string) *types.Message {
	key := MessageDBKey{ID: id, Topic: topic}
	data, closer, err := r.db.Get(key.Key())
	if err != nil {
		r.log.Error("failed to get message", zap.Error(err))
		return nil
	}
	defer closer.Close()

	return &types.Message{
		ID:    id,
		Data:  data,
		Topic: topic,
	}

}
