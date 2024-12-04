package pebble

import (
	"encoding/json"
	"errors"
	"eventura/modules/store"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
	"log"
	"strconv"
	"strings"
	"sync"
)

type PebbleEventStore struct {
	db            *pebble.DB
	eventSeqGen   *utils.SequenceGenerator
	eventsCh      chan store.EventRecord
	subscriptions map[string]*store.Subscription
	log           *zap.Logger
	subMutex      *sync.Mutex
	subAckMutex   *sync.Mutex
}

func (es *PebbleEventStore) Subscribe(consumerId string) (*store.Subscription, error) {
	//TODO implement me
	panic("implement me")
}

func (es *PebbleEventStore) GetSubscriptions() map[string]*store.Subscription {
	return es.subscriptions
}

/*func (es *PebbleEventStore) Subscribe(consumerGroup string) (*store.Subscription, error) {
	es.subMutex.Lock()
	defer es.subMutex.Unlock()

	if es.subscriptions[consumerGroup] != nil {
		es.log.Info("Subscription is active", zap.String("consumerGroup", consumerGroup))
		return nil, errors.New("subscription is active")
	}
	var sub *store.Subscription
	offset, closer, err := es.db.Get([]byte(fmt.Sprintf("subscription:%s", consumerGroup)))
	if errors.Is(err, pebble.ErrNotFound) {
		// No subscription offset found, start from the beginning
		es.db.Set([]byte(fmt.Sprintf("subscription:%s", consumerGroup)), []byte("0"), nil)
		sub = store.NewSubscription(consumerGroup, 0, es.log)
		es.subscriptions[consumerGroup] = sub
		return sub, nil
	}
	defer closer.Close()

	// Consumer offset found, start from the last acknowledged offset
	offsetInt, err := strconv.ParseUint(string(offset), 10, 64)
	if err != nil {
		es.log.Error("Failed to parse offset", zap.String("consumerGroup", consumerGroup), zap.String("offset", string(offset)))
		return nil, fmt.Errorf("failed to parse offset: %w", err)
	}
	sub = store.NewSubscription(consumerGroup, offsetInt, es.log)

	es.log.Info("Subscription started", zap.String("consumerGroup", consumerGroup), zap.Uint64("offset", offsetInt))
	go es.catchUpForSubscriber(sub, offsetInt)

	return es.subscriptions[consumerGroup], nil
}*/

func (es *PebbleEventStore) catchUpForSubscriber(sub *store.Subscription, startOffset uint64) {
	iter, _ := es.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("event:%d", startOffset+1)),
	})
	defer iter.Close() // Ensure iterator is closed

	for iter.First(); iter.Valid(); iter.Next() {
		if sub.IsStopped() {
			es.log.Info("Catch-up stopped for subscriber", zap.String("consumerGroup", sub.ConsumerGroup))
			return
		}

		eventID, err := strconv.ParseUint(string(iter.Key()[6:]), 10, 64)
		if err != nil {
			es.log.Error("Failed to parse event MessageID during catch-up", zap.String("key", string(iter.Key())))
			continue
		}

		event, err := es.GetEventByID(eventID)
		if err != nil {
			es.log.Error("Failed to fetch event during catch-up", zap.Uint64("eventID", eventID))
			continue
		}

		// Notify the subscriber
		sub.Notify(event)

		// Wait for acknowledgment
		for {
			if sub.CurrentOffset() >= eventID {
				break
			}
			if sub.IsStopped() {
				es.log.Info("Catch-up stopped during acknowledgment wait", zap.String("consumerGroup", sub.ConsumerGroup))
				return
			}
		}
	}

	if err := iter.Error(); err != nil {
		es.log.Error("Iterator error during catch-up", zap.Error(err))
	}
}

func (es *PebbleEventStore) Unsubscribe(consumerGroup string) {
	if es.subscriptions[consumerGroup] != nil {
		es.subscriptions[consumerGroup].Close()
		delete(es.subscriptions, consumerGroup)
	}
}

func (es *PebbleEventStore) NotifySubscribers(s *store.EventRecord) {
	for _, sub := range es.subscriptions {
		sub.Notify(s)
	}
}

// the client will call this method to acknowledge the event as processed.
func (es *PebbleEventStore) Acknowledge(consumerGroup string, eventID uint64) error {
	es.subAckMutex.Lock()
	defer es.subAckMutex.Unlock()

	err := es.db.Set([]byte(fmt.Sprintf("subscription:%s", consumerGroup)), []byte(fmt.Sprintf("%d", eventID)), nil)
	if err != nil {
		es.log.Error("Failed to acknowledge event", zap.String("consumerGroup", consumerGroup), zap.Uint64("eventID", eventID))
		return fmt.Errorf("failed to acknowledge event: %w", err)
	}

	return nil

}

func (es *PebbleEventStore) ReadAll() ([]store.EventRecord, error) {
	//TODO implement me
	panic("implement me")
}

func NewPebbleEventStore(path string, logger *zap.Logger) (store.EventStore, error) {
	opts := &pebble.Options{
		Cache:                 pebble.NewCache(512 << 20), // 512MB
		MemTableSize:          64 << 20,                   // 64MB
		L0CompactionThreshold: 4,
		L0StopWritesThreshold: 12,
		WALDir:                path + "/wal",
		MaxOpenFiles:          1000,
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open Pebble DB: %w",
			err,
		)
	}

	seqGen, err := utils.NewSequenceGenerator(db, "event-sequence", 100) // Initialize sequence generator
	if err != nil {
		return nil, fmt.Errorf(
			"failed to initialize sequence generator: %w",
			err,
		)
	}

	pebbleEventStore := &PebbleEventStore{
		db:            db,
		eventSeqGen:   seqGen,
		eventsCh:      make(chan store.EventRecord, 100),
		log:           logger,
		subscriptions: make(map[string]*store.Subscription),
		subMutex:      &sync.Mutex{},
	}

	return pebbleEventStore, nil

}

func (es *PebbleEventStore) GetEventByID(eventID uint64) (*store.EventRecord, error) {
	eventKey := fmt.Sprintf("event:%d", eventID)
	eventData, closer, err := es.db.Get([]byte(eventKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, fmt.Errorf("event not found: %w", err)
		}
		return nil, fmt.Errorf("failed to get event: %w", err)
	}
	defer utils.HandleAndLog(closer.Close, es.log)

	var event store.EventRecord
	if err := json.Unmarshal(eventData, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return &event, nil
}

func (es *PebbleEventStore) AppendEvent(event store.EventRecord) (uint64, error) {
	// Start batch
	batch := es.db.NewIndexedBatch()
	defer utils.HandleAndLog(batch.Close, es.log)

	// Generate unique event MessageID
	event.ID = es.eventSeqGen.Next()

	// Serialize the full event
	eventData, err := json.Marshal(event)
	if err != nil {
		log.Printf("AppendEvent: Error serializing event: %v", err)
		return 0, fmt.Errorf("failed to serialize event: %w", err)
	}

	// Store full event
	eventKey := fmt.Sprintf("event:%d", event.ID)
	if err := batch.Set([]byte(eventKey), eventData, nil); err != nil {
		log.Printf("AppendEvent: Error writing full event: %v", err)
		return 0, fmt.Errorf("failed to write full event: %w", err)
	}

	// Store by stream
	streamKey := fmt.Sprintf("stream:%s:%d", event.Stream, event.ID)
	if err := batch.Set([]byte(streamKey), []byte(fmt.Sprintf("%d", event.ID)), nil); err != nil {
		log.Printf("AppendEvent: Error writing index by_stream: %v", err)
		return 0, fmt.Errorf("failed to write index by_stream: %w", err)
	}

	for _, tagValue := range event.Metadata.Tags {
		tagKey := fmt.Sprintf("tag:%s:%d", tagValue, event.ID)
		if err := batch.Set([]byte(tagKey), []byte(fmt.Sprintf("%d", event.ID)), nil); err != nil {
			log.Printf("AppendEvent: Error adding tag '%s': %v", tagKey, err)
			return 0, fmt.Errorf("failed to add tag '%s': %w", tagKey, err)
		}
	}

	timeKey := fmt.Sprintf("time:%s:%d", event.Timestamp.Format("20060102150405"), event.ID)
	if err := batch.Set([]byte(timeKey), []byte(fmt.Sprintf("%d", event.ID)), nil); err != nil {
		return 0, fmt.Errorf("failed to write time index: %w", err)
	}

	// Commit the batch
	if err := batch.Commit(nil); err != nil {
		log.Printf("AppendEvent: Error committing batch: %v", err)
		return 0, fmt.Errorf("failed to commit batch: %w", err)
	}

	es.NotifySubscribers(&event)

	return event.ID, nil
}

// read by event type
func (es *PebbleEventStore) ReadByEventType(eventType string, fromEventID uint64) (*[]store.EventRecord, error) {
	var events []store.EventRecord

	// Define the prefix for the event type keys
	prefix := []byte(fmt.Sprintf("type:%s:", eventType))

	// Create an iterator for keys starting with the event type prefix
	iter, _ := es.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	defer utils.HandleAndLog(iter.Close, es.log)

	// Iterate through the keys matching the event type prefix
	for iter.SeekGE(prefix); iter.Valid() && strings.HasPrefix(string(iter.Key()), string(prefix)); iter.Next() { // Extract the event MessageID from the key or value
		// In this implementation, the value contains the event MessageID
		eventID, err := strconv.ParseUint(string(iter.Value()), 10, 64)
		if err != nil {
			log.Printf("Error parsing event MessageID for type %s: %v", eventType, err)
			continue
		}

		// Skip events below the starting point
		if eventID < fromEventID {
			continue
		}

		// Fetch the full event data using GetEventByID
		event, err := es.GetEventByID(eventID)
		if err != nil {
			log.Printf("Error fetching event for MessageID %d: %v", eventID, err)
			if !strings.Contains(err.Error(), "event not found") {
				return nil, fmt.Errorf("failed to fetch event for MessageID %d: %w", eventID, err)
			}
			// Skip missing events
			continue
		}

		// Append the resolved event to the result list
		events = append(events, *event)
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		log.Printf("Error iterating type %s: %v", eventType, err)
		return nil, fmt.Errorf("iterator error while reading type %s: %w", eventType, err)
	}

	return &events, nil
}

func (es *PebbleEventStore) ReadByTag(tag string, fromEventID uint64) (*[]store.EventRecord, error) {
	var events []store.EventRecord

	prefix := []byte(fmt.Sprintf("tag:%s:", tag))
	iter, _ := es.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	defer utils.HandleAndLog(iter.Close, es.log)

	for iter.SeekGE(prefix); iter.Valid() && strings.HasPrefix(string(iter.Key()), string(prefix)); iter.Next() {
		eventID, err := strconv.ParseUint(string(iter.Value()), 10, 64)
		if err != nil {
			es.log.Error("Error parsing event MessageID for tag", zap.String("tag", tag), zap.Error(err))
			continue
		}

		if eventID < fromEventID {
			continue
		}

		event, err := es.GetEventByID(eventID)
		if err != nil {
			es.log.Error("Failed to fetch event for MessageID", zap.Uint64("eventID", eventID), zap.Error(err))
			if !errors.Is(err, pebble.ErrNotFound) {
				return nil, fmt.Errorf("failed to fetch event for MessageID %d: %w", eventID, err)
			}
			continue
		}

		events = append(events, *event)
	}

	if err := iter.Error(); err != nil {
		es.log.Error("Iterator error during ReadByTag", zap.Error(err))
		return nil, fmt.Errorf("iterator error while reading tag: %w", err)
	}

	return &events, nil
}

func (es *PebbleEventStore) ReadByStream(stream string, limit uint64) (*[]store.EventRecord, error) {
	var events []store.EventRecord

	// Define bounds for the index keys of the specific stream
	lowerBound := []byte(fmt.Sprintf("stream:%s:", stream))
	upperBound := []byte(fmt.Sprintf("stream:%s;", stream)) // Use `;` as a terminator

	// Create an iterator for the stream range
	iter, err := es.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator for stream %s: %w", stream, err)
	}
	defer iter.Close()

	// Iterate through the index keys for the stream
	for iter.SeekGE(lowerBound); iter.Valid(); iter.Next() {
		// Extract the event MessageID from the value of the index key
		eventID, err := strconv.ParseUint(string(iter.Value()), 10, 64)
		if err != nil {
			log.Printf("Error parsing event MessageID for key %s: %v", iter.Key(), err)
			continue
		}

		// Fetch the full event data using GetEventByID
		event, err := es.GetEventByID(eventID)
		if err != nil {
			log.Printf("Error fetching event for MessageID %d: %v", eventID, err)
			if !strings.Contains(err.Error(), "event not found") {
				return nil, fmt.Errorf("failed to fetch event for MessageID %d: %w", eventID, err)
			}
			// Skip missing events
			continue
		}

		// Append the resolved event to the result list
		events = append(events, *event)
		if len(events) == int(limit) {
			break
		}
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		log.Printf("Error iterating stream %s: %v", stream, err)
		return nil, fmt.Errorf("iterator error while reading stream %s: %w", stream, err)
	}

	return &events, nil
}

// Close closes the database.
func (es *PebbleEventStore) Close() error {
	if err := es.db.Close(); err != nil {
		log.Printf("Error closing database: %v", err)
		return err
	}
	return nil
}
