package internal

import (
	"encoding/json"
	"eventura/modules/pubsy/core"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"math"
	"sync"
	"time"
)

const (
	KEY_SPACE               = "t_messages"
	INDEX_MESSAGE_TIMESTAMP = "index_message_timestamp"
)

type TopicMessageRepositoryImpl struct {
	sequenceCache map[string]*utils.SequenceGenerator //If this causes issue place on topic
	seqMutex      *sync.Mutex
	db            *pebble.DB
	log           *zap.Logger
}

func (r *TopicMessageRepositoryImpl) getNextForTopic(topic string) (uint64, error) {
	r.seqMutex.Lock()
	defer r.seqMutex.Unlock()
	var seq *utils.SequenceGenerator
	var err error
	// New topic needs a new sequence generator
	if r.sequenceCache[topic] == nil {
		seq, err = utils.NewSequenceGenerator(r.db, topic, 10)
		if err != nil {
			r.log.Error("Error creating sequence generator", zap.Error(err))
			return 0, fmt.Errorf("error creating sequence generator: %w", err)
		}
		r.sequenceCache[topic] = seq
	} else {
		seq = r.sequenceCache[topic]
	}

	return seq.Next(), nil
}

func (r *TopicMessageRepositoryImpl) Save(topic string, data []byte) (*core.TopicMessage, error) {

	id, err := r.getNextForTopic(topic)

	if err != nil {
		return nil, err
	}
	key := utils.NewCompositeKey(KEY_SPACE).
		AddString(topic).
		AddUint64(id).
		Build()

	topicMessage := &core.TopicMessage{
		ID:      id,
		Data:    data,
		Topic:   topic,
		Created: time.Now().UTC(),
	}

	jsonData, err := json.Marshal(topicMessage)
	if err != nil {
		r.log.Error("Error marshalling message", zap.Error(err))
		return nil, fmt.Errorf("error marshalling message: %w", err)
	}

	err = r.db.Set(key, jsonData, pebble.Sync)
	if err != nil {
		r.log.Error("Error saving message", zap.Error(err))
		return nil, fmt.Errorf("error saving message: %w", err)
	}

	err = r.createMessageTimestampIndex(topicMessage)
	if err != nil {
		r.log.Error("Error creating message timestamp index", zap.Error(err))
		return nil, fmt.Errorf("error creating message timestamp index: %w", err)

	}

	return topicMessage, nil
}

// createMessageTimestampIndex creates an index for the message timestamp so that we can query messages by timestamp
func (r *TopicMessageRepositoryImpl) createMessageTimestampIndex(topicMessage *core.TopicMessage) error {
	key := utils.NewCompositeKey(INDEX_MESSAGE_TIMESTAMP).
		AddUint64(uint64(topicMessage.Created.UnixNano())).
		AddString(topicMessage.Topic).
		Build()

	err := r.db.Set(key, utils.UintToBytes(topicMessage.ID), pebble.Sync)

	if err != nil {
		r.log.Error("Error creating message timestamp index", zap.Error(err))
		return fmt.Errorf("error creating message timestamp index: %w", err)
	}
	return nil
}

func (r *TopicMessageRepositoryImpl) GetMessagesFromOffset(topic string, offset uint64, limit uint64) (*[]core.TopicMessage, error) {
	lowerBound := utils.NewCompositeKey(KEY_SPACE).
		AddString(topic).
		AddUint64(offset).
		Build()

	upperBound := utils.NewCompositeKey(KEY_SPACE).
		AddString(topic).
		AddUint64(math.MaxUint64).
		Build()

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		r.log.Error("Error getting messages from offset", zap.Error(err))
		return nil, fmt.Errorf("error getting messages from offset: %w", err)
	}
	defer iter.Close()

	var messages []core.TopicMessage
	count := uint64(0)

	for valid := iter.First(); valid && iter.Valid(); valid = iter.Next() {
		if count >= limit {
			break
		}

		data, err := iter.ValueAndErr()
		if err != nil {
			r.log.Error("Error getting message data", zap.Error(err))
			return nil, fmt.Errorf("error getting message data: %w", err)
		}

		var tm core.TopicMessage
		if err := json.Unmarshal(data, &tm); err != nil {
			r.log.Error("Error unmarshalling JSON message", zap.Error(err))
			return nil, fmt.Errorf("error unmarshalling JSON message: %w", err)
		}

		messages = append(messages, tm)
		count++
	}

	return &messages, nil
}

func (r *TopicMessageRepositoryImpl) GetMessage(topic string, id uint64) (*core.TopicMessage, error) {
	key := utils.NewCompositeKey(KEY_SPACE).
		AddString(topic).
		AddUint64(id).
		Build()

	data, closer, err := r.db.Get(key)
	defer closer.Close()
	if err != nil {
		r.log.Error("Error getting message", zap.Error(err))
		return nil, fmt.Errorf("error getting message: %w", err)
	}

	var tm core.TopicMessage
	if err := json.Unmarshal(data, &tm); err != nil {
		r.log.Error("Error unmarshalling JSON message", zap.Error(err))
		return nil, fmt.Errorf("error unmarshalling JSON message: %w", err)
	}

	return &tm, nil
}

func (r *TopicMessageRepositoryImpl) DeleteMessagesOlderThan(topic string, duration time.Duration) (count uint64, err error) {
	// Compute the cutoff timestamp
	cutoff := time.Now().Add(-duration).UnixNano()

	// Construct iteration bounds
	lowerBound := utils.NewCompositeKey(INDEX_MESSAGE_TIMESTAMP).
		AddUint64(0).
		AddString(topic).
		Build()

	upperBound := utils.NewCompositeKey(INDEX_MESSAGE_TIMESTAMP).
		AddUint64(uint64(cutoff)).
		AddString(topic).
		Build()

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		r.log.Error("Error creating iterator for DeleteMessagesOlderThan", zap.Error(err))
		return 0, fmt.Errorf("error creating iterator: %w", err)
	}
	defer iter.Close()

	batch := r.db.NewBatch()
	defer batch.Close()

	count = 0

	for valid := iter.First(); valid && iter.Valid(); valid = iter.Next() {
		count++
		val, err := iter.ValueAndErr()
		if err != nil {
			r.log.Error("Error reading value from timestamp index", zap.Error(err))
			return 0, fmt.Errorf("error reading value from index: %w", err)
		}

		msgID, err := utils.BytesToUint(val)
		if err != nil {
			r.log.Error("Error parsing message ID from index value", zap.Error(err))
			return 0, fmt.Errorf("error parsing message ID: %w", err)
		}

		// Construct the main message key: KEY_SPACE:topic:id
		mainKey := utils.NewCompositeKey(KEY_SPACE).
			AddString(topic).
			AddUint64(msgID).
			Build()

		// Delete the main key
		if err := batch.Delete(mainKey, nil); err != nil {
			r.log.Error("Error scheduling main key deletion", zap.Error(err))
			return 0, fmt.Errorf("error deleting main key: %w", err)
		}

		// Delete the index key
		idxKey := iter.Key()
		idxKeyCopy := make([]byte, len(idxKey))
		copy(idxKeyCopy, idxKey)
		if err := batch.Delete(idxKeyCopy, nil); err != nil {
			r.log.Error("Error scheduling index key deletion", zap.Error(err))
			return 0, fmt.Errorf("error deleting index key: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		r.log.Error("Error committing batch deletion", zap.Error(err))
		return 0, fmt.Errorf("error committing batch: %w", err)
	}

	r.log.Info("Deleted messages older than cutoff", zap.Uint64("count", count))
	return count, nil
}

func (r *TopicMessageRepositoryImpl) GetLastMessageID(topic string) (uint64, error) {

	upperBound := utils.NewCompositeKey(KEY_SPACE).
		AddString(topic).
		AddUint64(math.MaxUint64).
		Build()

	// Create an iterator
	iter, err := r.db.NewIter(&pebble.IterOptions{
		UpperBound: upperBound,
	})
	if err != nil {
		r.log.Error("Error creating iterator for GetLastMessageID", zap.Error(err))
		return 0, fmt.Errorf("error creating iterator: %w", err)
	}
	defer iter.Close()

	// Seek to the last key
	if iter.Last() {
		ck := utils.NewCompositeKey(KEY_SPACE)
		errKf := ck.Parse(iter.Key())
		if errKf != nil {
			r.log.Error("Error parsing key", zap.Error(errKf))
			return 0, fmt.Errorf("error parsing key: %w", errKf)
		}
		ck.GetString() // trigger cursor to move to next field
		offset, errKf := ck.GetUint64()
		if errKf != nil {
			r.log.Error("Error getting uint64 from key", zap.Error(errKf))
			return 0, fmt.Errorf("error getting uint64 from key: %w", errKf)
		}

		return offset, nil
	}

	// If no keys were found, return an error
	r.log.Error("No messages found for the given topic")
	return 0, nil
}

func NewTopicRepositoryImpl(db *pebble.DB, log *zap.Logger) core.TopicRepository {
	return &TopicMessageRepositoryImpl{
		db:            db,
		log:           log,
		sequenceCache: make(map[string]*utils.SequenceGenerator),
		seqMutex:      &sync.Mutex{},
	}

}

func topicMsgMainKey(topic string, id uint64) []byte {
	return utils.NewCompositeKey(KEY_SPACE).AddString(topic).AddUint64(id).Build()
}