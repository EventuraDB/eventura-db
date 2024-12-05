package pubsub

import (
	"eventura/modules/pubsub2/domain"
	"eventura/modules/pubsub2/infra"
	"github.com/cockroachdb/pebble"
	"sync"
)

// Message represents a message published to a topic_.
type Message struct {
	ID   string
	Data []byte
}

// MessageHandler is a function to process messages.
type MessageHandler func(msg Message) error

// Broker manages topics and subscribers.
type Broker struct {
	db          *pebble.DB
	topics      map[string]*Topic
	mu          sync.RWMutex
	messageRepo *domain.MessageRepository
}

// Topic represents a single topic_ with subscribers and messages.
type Topic struct {
	name        string
	subscribers map[string]*Subscriber
	mu          sync.RWMutex
}

// Subscriber represents a consumer subscribed to a topic_.
type Subscriber struct {
	ID       string
	offset   string
	callback MessageHandler
}

// NewBroker initializes a new Broker.
func NewBroker(db *pebble.DB) *Broker {

	repo := infra.NewPebbleMessageRepository()

	return &Broker{
		db:          db,
		topics:      make(map[string]*Topic),
		messageRepo: &repo,
	}
}

// CreateTopic creates a new topic_.
func (b *Broker) CreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; !exists {
		b.topics[name] = &Topic{
			name:        name,
			subscribers: make(map[string]*Subscriber),
		}
	}
	return b.topics[name]
}

// Publish publishes a message to a topic_.
func (b *Broker) Publish(topicName string, msg Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, exists := b.topics[topicName]
	if !exists {
		return nil // Topic does not exist; no-op.
	}
	return topic.publish(msg, b.db)
}

// Subscribe subscribes a consumer to a topic_.
func (b *Broker) Subscribe(topicName, subscriberID string, callback MessageHandler) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, exists := b.topics[topicName]
	if !exists {
		return nil // Topic does not exist; no-op.
	}
	topic.addSubscriber(subscriberID, callback)
	return nil
}

// publish adds a message to the topic_ and notifies subscribers.
func (t *Topic) publish(msg Message, db *pebble.DB) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Store message in DB
	key := []byte(t.name + "/" + msg.ID)
	if err := db.Set(key, msg.Data, pebble.Sync); err != nil {
		return err
	}

	// Notify subscribers
	for _, sub := range t.subscribers {
		go func(s *Subscriber) {
			if err := s.callback(msg); err != nil {
				// Handle subscriber errors (e.g., retry logic, logging, etc.)
			}
		}(sub)
	}
	return nil
}

// addSubscriber adds a subscriber to the topic_.
func (t *Topic) addSubscriber(id string, callback MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.subscribers[id] = &Subscriber{
		ID:       id,
		offset:   "",
		callback: callback,
	}
}
