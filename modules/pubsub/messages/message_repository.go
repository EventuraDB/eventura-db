package messages

import (
	"bytes"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

const (
	MESSAGES_NAMESPACE = "messages"
)

type MessageRepository struct {
	db  *pebble.DB
	log *zap.Logger
}

type MessageRecord struct {
	ID    uint64
	Data  []byte
	Topic string
}

type MessageDBKey struct {
	Topic string
	ID    uint64
}

func (m *MessageDBKey) Key() []byte {
	return []byte(fmt.Sprintf("%s:%s:%d", MESSAGES_NAMESPACE, m.Topic, utils.UintToBytes(m.ID)))
}

type MessageDBValue struct {
	Data []byte `json:"data"`
}

func (v MessageDBValue) Value() []byte {
	return v.Data
}

func NewMessageRepository(db *pebble.DB, log *zap.Logger) *MessageRepository {
	return &MessageRepository{
		db:  db,
		log: log,
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

// find messages in topic starting from a give offset
func (r *MessageRepository) GetMessagesInTopicFromOffset(topic string, offset uint64, limit uint64) ([]MessageRecord, error) {

	lowerBound := MessageDBKey{topic, offset}
	upperBound := MessageDBKey{topic, offset + limit}

	iter, _ := r.db.NewIter(
		&pebble.IterOptions{
			LowerBound: lowerBound.Key(),
			UpperBound: upperBound.Key(),
		})

	defer utils.HandleAndLog(iter.Close, r.log)

	count := 0
	var messages []MessageRecord
	for iter.First(); iter.Valid(); iter.Next() {
		dbKey := fromDBKey(iter.Key())

		if dbKey.ID < offset {
			continue
		}

		count++
		if count >= int(limit) {
			break
		}

		messages = append(messages, MessageRecord{
			ID:    dbKey.ID,
			Data:  iter.Value(),
			Topic: dbKey.Topic,
		})
	}

	return messages, nil
}

func (r *MessageRepository) GetMessagesInTopic(topic string, limit uint64) ([]MessageRecord, error) {
	iter, _ := r.db.NewIter(nil)
	defer iter.Close()

	prefix := fmt.Sprintf("%s:%s:", MESSAGES_NAMESPACE, topic)

	count := 0
	var messages []MessageRecord
	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		count++
		if count >= int(limit) {
			break
		}
		dbKey := fromDBKey(iter.Key())

		messages = append(messages, MessageRecord{
			ID:    dbKey.ID,
			Data:  iter.Value(),
			Topic: dbKey.Topic,
		})
	}

	return messages, nil
}
func (r *MessageRepository) InsertMessage(topic string, data []byte) error {
	generator, err := utils.NewSequenceGenerator(r.db, MESSAGES_NAMESPACE, 10)
	if err != nil {
		return err
	}

	message := MessageRecord{
		ID:    generator.Next(),
		Data:  data,
		Topic: topic,
	}

	dbValue := MessageDBValue{Data: message.Data}

	dbKey := MessageDBKey{message.Topic, message.ID}

	err = r.db.Set(dbKey.Key(), dbValue.Value(), pebble.Sync)
	if err != nil {
		return err
	}
	return nil
}
