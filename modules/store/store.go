package store

import (
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/jsonpath"
	"github.com/cockroachdb/pebble"
	"github.com/nats-io/nats-server/v2/server"
	"log"
	"strconv"
	"strings"
)

type EventStore struct {
	db               *pebble.DB
	eventSeqGen      *SequenceGenerator
	indexDefinitions map[string]map[string]IndexDefinition
}

func NewEventStore(path string, ns *server.Server) (*EventStore, error) {
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
		return nil, fmt.Errorf("failed to open Pebble DB: %w", err)
	}

	seqGen, err := NewSequenceGenerator(db, "event-sequence", 100) // Initialize sequence generator
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sequence generator: %w", err)
	}

	store := &EventStore{
		db:               db,
		eventSeqGen:      seqGen, // Assign here
		indexDefinitions: make(map[string]map[string]IndexDefinition),
	}

	// Load pre-existing indexes
	if err := store.LoadIndexes(); err != nil {
		return nil, fmt.Errorf("failed to load indexes: %w", err)
	}

	return store, nil
}

func (es *EventStore) LoadIndexes() error {
	iter, _ := es.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("index_definitions:"),
		UpperBound: []byte("index_definitions;"),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		indexData, err := iter.ValueAndErr()
		if err != nil {
			log.Printf("Error reading index definition: %v", err)
			continue
		}

		var config IndexConfig
		if err := json.Unmarshal(indexData, &config); err != nil {
			log.Printf("Error unmarshaling index definition: %v", err)
			continue
		}

		if es.indexDefinitions == nil {
			es.indexDefinitions = make(map[string]map[string]IndexDefinition)
		}

		if _, exists := es.indexDefinitions[config.EventType]; !exists {
			es.indexDefinitions[config.EventType] = make(map[string]IndexDefinition)
		}

		es.indexDefinitions[config.EventType][config.IndexName] = IndexDefinition{
			JSONPath: config.JSONPath,
		}

		log.Printf("Loaded index '%s' for event type '%s' with JSON Path '%s'", config.IndexName, config.EventType, config.JSONPath)
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("error iterating index definitions: %w", err)
	}

	return nil
}

func (es *EventStore) GetEvent(eventID uint64) (*Event, error) {
	eventKey := fmt.Sprintf("event:%d", eventID)
	eventData, closer, err := es.db.Get([]byte(eventKey))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("event not found: %w", err)
		}
		return nil, fmt.Errorf("failed to get event: %w", err)
	}
	defer closer.Close()

	var event Event
	if err := json.Unmarshal(eventData, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	return &event, nil
}

func (es *EventStore) StoreEvent(event *Event) (uint64, error) {
	// Start batch
	batch := es.db.NewBatch()
	defer batch.Close()

	// Generate unique event ID
	event.ID = es.eventSeqGen.GetNextSequence()

	// Serialize the full event
	eventData, err := json.Marshal(event)
	if err != nil {
		log.Printf("StoreEvent: Error serializing event: %v", err)
		return 0, fmt.Errorf("failed to serialize event: %w", err)
	}

	// Store full event
	eventKey := fmt.Sprintf("event:%d", event.ID)
	if err := batch.Set([]byte(eventKey), eventData, nil); err != nil {
		log.Printf("StoreEvent: Error writing full event: %v", err)
		return 0, fmt.Errorf("failed to write full event: %w", err)
	}

	// Apply all relevant indexes
	for eventType, indexes := range es.indexDefinitions {
		if eventType != event.Type {
			continue // Skip indexes not related to this event type
		}

		for indexName, indexDef := range indexes {
			// Extract value using the JSON Path
			value, err := extractValueFromPayload(event.Payload, indexDef.JSONPath)
			if err != nil {
				log.Printf("StoreEvent: Error extracting value for index '%s': %v", indexName, err)
				continue
			}

			// Create index key
			indexKey := fmt.Sprintf("index:%s:%s:%d", indexName, value, event.ID)
			if err := batch.Set([]byte(indexKey), []byte(fmt.Sprintf("%d", event.ID)), nil); err != nil {
				log.Printf("StoreEvent: Error adding index '%s': %v", indexName, err)
				return 0, fmt.Errorf("failed to add index '%s': %w", indexName, err)
			}
		}
	}

	// Commit the batch
	if err := batch.Commit(nil); err != nil {
		log.Printf("StoreEvent: Error committing batch: %v", err)
		return 0, fmt.Errorf("failed to commit batch: %w", err)
	}

	log.Printf("StoreEvent: Successfully stored event with ID %d", event.ID)
	return event.ID, nil
}

// Helper function to extract value from JSON payload using a JSON path
func extractValueFromPayload(payload []byte, jsonPath string) (string, error) {
	var jsonData map[string]interface{}
	if err := json.Unmarshal(payload, &jsonData); err != nil {
		return "", fmt.Errorf("failed to unmarshal JSON payload: %w", err)
	}

	// Use a simple JSON path library or write a custom parser
	value, err := jsonpath.Get(jsonPath, jsonData)
	if err != nil {
		return "", fmt.Errorf("failed to extract value using JSON path '%s': %w", jsonPath, err)
	}

	return fmt.Sprintf("%v", value), nil
}

/*func (es *EventStore) SubscribeAll(consumerID string, callback func(event *Event) error) error {
	subscription, err := es.js.Subscribe("_all", func(msg *nats.Msg) {
		// Parse the event ID from the message
		eventID, err := strconv.ParseUint(string(msg.Data), 10, 64)
		if err != nil {
			log.Printf("Failed to parse event ID: %v", err)
			_ = msg.Nak() // Negative acknowledgment to reprocess later
			return
		}

		// Fetch the full event data using GetEvent
		event, err := es.GetEvent(eventID)
		if err != nil {
			if strings.Contains(err.Error(), "event not found") {
				log.Printf("Event ID %d not found, skipping: %v", eventID, err)
				_ = msg.Ack() // Acknowledge to avoid reprocessing
				return
			}
			log.Printf("Failed to retrieve event for ID %d: %v", eventID, err)
			_ = msg.Nak() // Negative acknowledgment to reprocess later
			return
		}

		// Call the callback with the full event
		err = callback(event)
		if err != nil {
			log.Printf("Callback failed for event ID %d: %v", eventID, err)
			_ = msg.Nak() // Retry later on callback failure
			return
		}

		// Acknowledge the message
		if err := msg.Ack(); err != nil {
			log.Printf("Failed to acknowledge message for event ID %d: %v", eventID, err)
		}
	}, nats.Durable(consumerID))
	if err != nil {
		return fmt.Errorf("failed to subscribe to _all stream: %w", err)
	}

	// Ensure the subscription is cleaned up when the context is canceled
	go func() {
		<-ctx.Done()
		if err := subscription.Unsubscribe(); err != nil {
			log.Printf("Failed to unsubscribe from _all stream: %v", err)
		}
	}()

	return nil
}
func (es *EventStore) ensureStream(streamName string, subject string) error {
	// Check if the stream already exists
	_, err := es.js.StreamInfo(streamName)
	if err == nil {
		// log.Printf("Stream %s already exists", streamName)
		return nil
	}

	if err != nats.ErrStreamNotFound {
		return fmt.Errorf("error checking stream: %w", err)
	}

	// Stream does not exist, so create it
	log.Printf("Creating stream %s for subject %s", streamName, subject)
	_, err = es.js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Storage:   nats.FileStorage,     // or nats.MemoryStorage
		Retention: nats.WorkQueuePolicy, // Change based on requirements
	})
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}*/

func (es *EventStore) AddIndex(config IndexConfig) error {
	// Validate inputs
	if config.IndexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if config.EventType == "" {
		return fmt.Errorf("event type cannot be empty")
	}
	if config.JSONPath == "" {
		return fmt.Errorf("JSON path cannot be empty")
	}

	// Ensure the index definitions map is initialized
	if es.indexDefinitions == nil {
		es.indexDefinitions = make(map[string]map[string]IndexDefinition)
	}

	// Ensure the map for the event type is initialized
	if _, exists := es.indexDefinitions[config.EventType]; !exists {
		es.indexDefinitions[config.EventType] = make(map[string]IndexDefinition)
	}

	// Check if the index already exists
	if _, exists := es.indexDefinitions[config.EventType][config.IndexName]; exists {
		return fmt.Errorf("index with name '%s' already exists for event type '%s'", config.IndexName, config.EventType)
	}

	// Persist the index definition in the database
	indexKey := fmt.Sprintf("index_definitions:%s:%s", config.EventType, config.IndexName)
	indexData, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to serialize index definition: %w", err)
	}

	if err := es.db.Set([]byte(indexKey), indexData, pebble.Sync); err != nil {
		return fmt.Errorf("failed to persist index definition: %w", err)
	}

	// Add the index definition to the in-memory map
	es.indexDefinitions[config.EventType][config.IndexName] = IndexDefinition{
		JSONPath: config.JSONPath,
	}

	log.Printf("Index '%s' added for event type '%s' with JSON Path '%s'", config.IndexName, config.EventType, config.JSONPath)
	return nil
}

func (es *EventStore) ReadBySubject(subject string, fromEventID uint64) ([]Event, error) {
	var events []Event

	// Normalize the subject by removing the wildcard (*) if present
	normalizedSubject := strings.TrimSuffix(subject, "*")

	// Define the prefix for iteration directly based on the normalized subject
	prefix := []byte(fmt.Sprintf("index:%s", normalizedSubject))

	// Create an iterator starting at the normalized subject prefix
	iter, _ := es.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	defer iter.Close()

	// Iterate through the index keys matching the normalized subject
	for iter.SeekGE(prefix); iter.Valid() && strings.HasPrefix(string(iter.Key()), string(prefix)); iter.Next() {
		// Extract the event ID from the value of the index key
		value, err := iter.ValueAndErr()
		if err != nil {
			log.Printf("Error reading value for key %s: %v", iter.Key(), err)
			continue
		}

		eventID, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			log.Printf("Error parsing event ID for key %s: %v", iter.Key(), err)
			continue
		}

		// Skip events below the starting point
		if eventID < fromEventID {
			continue
		}

		// Fetch the full event data using GetEvent
		event, err := es.GetEvent(eventID)
		if err != nil {
			log.Printf("Error fetching event for ID %d: %v", eventID, err)
			if !strings.Contains(err.Error(), "event not found") {
				return nil, fmt.Errorf("failed to fetch event for ID %d: %w", eventID, err)
			}
			// Skip missing events
			continue
		}

		// Append the resolved event to the result list
		events = append(events, *event)
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		log.Printf("Error iterating subject indexes: %v", err)
		return nil, fmt.Errorf("iterator error while reading subject indexes: %w", err)
	}

	return events, nil
}

func (es *EventStore) ReadStream(streamID string) ([]Event, error) {
	var events []Event

	// Define bounds for the index keys of the specific stream
	lowerBound := []byte(fmt.Sprintf("index:by_stream_id:%s:", streamID))
	upperBound := []byte(fmt.Sprintf("index:by_stream_id:%s;", streamID)) // Use `;` as a terminator

	// Create an iterator for the stream range
	iter, err := es.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator for stream %s: %w", streamID, err)
	}
	defer iter.Close()

	// Iterate through the index keys for the stream
	for iter.SeekGE(lowerBound); iter.Valid(); iter.Next() {
		// Extract the event ID from the value of the index key
		eventID, err := strconv.ParseUint(string(iter.Value()), 10, 64)
		if err != nil {
			log.Printf("Error parsing event ID for key %s: %v", iter.Key(), err)
			continue
		}

		// Fetch the full event data using GetEvent
		event, err := es.GetEvent(eventID)
		if err != nil {
			log.Printf("Error fetching event for ID %d: %v", eventID, err)
			if !strings.Contains(err.Error(), "event not found") {
				return nil, fmt.Errorf("failed to fetch event for ID %d: %w", eventID, err)
			}
			// Skip missing events
			continue
		}

		// Append the resolved event to the result list
		events = append(events, *event)
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		log.Printf("Error iterating stream %s: %v", streamID, err)
		return nil, fmt.Errorf("iterator error while reading stream %s: %w", streamID, err)
	}

	return events, nil
}

// Close closes the database.
func (es *EventStore) Close() error {
	if err := es.db.Close(); err != nil {
		log.Printf("Error closing database: %v", err)
		return err
	}
	return nil
}
