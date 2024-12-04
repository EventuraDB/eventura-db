package subscription

import (
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

type SubscriptionDBKey struct {
	ConsumerID string
	Topic      string
}

func (k SubscriptionDBKey) Key() []byte {
	return []byte(fmt.Sprintf("sub:%s:%s", k.ConsumerID, k.Topic))
}

type SubscriptionDBValue struct {
	ConsumerID string `json:"consumer_id"`
	Offset     uint64 `json:"offset"`
}

type SubscriptionRecord struct {
	ConsumerID string
	Topic      string
	Offset     uint64
}

func (v SubscriptionDBValue) Value() []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}

type SubscriptionRepository struct {
	db  *pebble.DB
	log *zap.Logger
}

func NewSubscriptionRepository(db *pebble.DB, log *zap.Logger) *SubscriptionRepository {
	return &SubscriptionRepository{
		db:  db,
		log: log,
	}
}

// check if subscription exists
func (r *SubscriptionRepository) Exists(consumerId string, topic string) bool {
	key := SubscriptionDBKey{
		ConsumerID: consumerId,
		Topic:      topic,
	}

	data, closer, err := r.db.Get(key.Key())
	if err != nil {
		return false
	}
	defer closer.Close()

	return data != nil
}

// Find subscription by consumerId and topic
func (r *SubscriptionRepository) Find(consumerId string, topic string) (*SubscriptionRecord, error) {
	key := SubscriptionDBKey{
		ConsumerID: consumerId,
		Topic:      topic,
	}

	data, closer, err := r.db.Get(key.Key())
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var value SubscriptionDBValue
	err = json.Unmarshal(data, &value)
	if err != nil {
		return nil, err
	}

	return &SubscriptionRecord{
		ConsumerID: value.ConsumerID,
		Topic:      topic,
		Offset:     value.Offset,
	}, nil
}

func (r *SubscriptionRepository) Insert(consumerId string, topic string) (*SubscriptionRecord, error) {
	key := SubscriptionDBKey{
		ConsumerID: consumerId,
		Topic:      topic,
	}

	value := SubscriptionDBValue{
		ConsumerID: consumerId,
		Offset:     0,
	}

	err := r.db.Set(key.Key(), value.Value(), pebble.Sync)
	if err != nil {
		return nil, err
	}

	return &SubscriptionRecord{
		ConsumerID: consumerId,
		Topic:      topic,
		Offset:     0,
	}, nil
}

// Update subscription offset
func (r *SubscriptionRepository) UpdateOffset(consumerId string, topic string, offset uint64) error {
	key := SubscriptionDBKey{
		ConsumerID: consumerId,
		Topic:      topic,
	}

	value := SubscriptionDBValue{
		ConsumerID: consumerId,
		Offset:     offset,
	}

	return r.db.Set(key.Key(), value.Value(), pebble.Sync)
}
