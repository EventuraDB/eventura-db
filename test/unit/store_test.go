package store_test

import (
	"encoding/json"
	"eventura/modules/store"
	"eventura/modules/store/pebble"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEventStoreWithMultipleEvents(t *testing.T) {
	// Create a temporary directory for the database
	dbPath := t.TempDir()

	// Initialize PebbleEventStore
	es, err := pebble.NewPebbleEventStore(filepath.Join(dbPath, "pebble"), nil)
	assert.NoError(t, err)
	defer es.Close()

	// Define multiple test events
	events := []store.EventRecord{
		store.NewEventRecord("test-stream-1", "TypeA", []byte(`{"field":"valueA"}`), []string{"test"}),
		store.NewEventRecord("test-stream-1", "TypeB", []byte(`{"field":"valueB"}`), []string{"test"}),
		store.NewEventRecord("test-stream-2", "TypeA", []byte(`{"field":"valueC"}`), []string{"example"}),
		store.NewEventRecord("test-stream-3", "TypeC", []byte(`{"field":"valueD"}`), []string{"example"}),
	}

	// Store all events and collect their IDs
	var eventIDs []uint64
	for i, event := range events {
		eventID, err := es.AppendEvent(event)
		assert.NoError(t, err)
		assert.Greater(t, eventID, uint64(0), "EventRecord MessageID for event %d should be greater than 0", i)
		eventIDs = append(eventIDs, eventID)
	}

	// Test retrieving events by MessageID
	for i, eventID := range eventIDs {
		retrievedEvent, err := es.GetEventByID(eventID)
		assert.NoError(t, err, "Failed to retrieve event by MessageID for event %d", i)
		assert.NotNil(t, retrievedEvent, "Retrieved event should not be nil for event %d", i)
		assert.Equal(t, eventID, retrievedEvent.ID, "EventRecord MessageID mismatch for event %d", i)
		assert.Equal(t, events[i].Stream, retrievedEvent.Stream, "Stream mismatch for event %d", i)
		assert.Equal(t, events[i].Type, retrievedEvent.Type, "Type mismatch for event %d", i)
		assert.Equal(t, events[i].Event, retrievedEvent.Event, "Event mismatch for event %d", i)
		assert.ElementsMatch(t, events[i].Metadata.Tags, retrievedEvent.Metadata.Tags, "Metadata tags mismatch for event %d", i)
	}

	// Test retrieving events by stream
	stream1Events, err := es.ReadByStream("test-stream-1", 0)
	assert.NoError(t, err)
	assert.Len(t, *stream1Events, 2)
	assert.Equal(t, eventIDs[0], (*stream1Events)[0].ID)
	assert.Equal(t, eventIDs[1], (*stream1Events)[1].ID)

	stream2Events, err := es.ReadByStream("test-stream-2", 0)
	assert.NoError(t, err)
	assert.Len(t, *stream2Events, 1)
	assert.Equal(t, eventIDs[2], (*stream2Events)[0].ID)

	// Test retrieving events by tag
	testTaggedEvents, err := es.ReadByTag("test", 0)
	assert.NoError(t, err)
	assert.Len(t, *testTaggedEvents, 2)
	assert.Contains(t, []uint64{eventIDs[0], eventIDs[1]}, (*testTaggedEvents)[0].ID)
	assert.Contains(t, []uint64{eventIDs[0], eventIDs[1]}, (*testTaggedEvents)[1].ID)

	exampleTaggedEvents, err := es.ReadByTag("example", 0)
	assert.NoError(t, err)
	assert.Len(t, *exampleTaggedEvents, 2)
	assert.Contains(t, []uint64{eventIDs[2], eventIDs[3]}, (*exampleTaggedEvents)[0].ID)
	assert.Contains(t, []uint64{eventIDs[2], eventIDs[3]}, (*exampleTaggedEvents)[1].ID)

	// Print events for debugging
	for i, eventID := range eventIDs {
		retrievedEvent, _ := es.GetEventByID(eventID)
		eventJSON, _ := json.Marshal(retrievedEvent)
		t.Logf("EventRecord %d: %s", i, eventJSON)
	}
}

func TestEventStoreWithSubscriptions(t *testing.T) {
	// Create a temporary directory for the database
	dbPath := t.TempDir()

	// Initialize PebbleEventStore
	es, err := pebble.NewPebbleEventStore(filepath.Join(dbPath, "pebble"), nil)
	assert.NoError(t, err)
	defer es.Close()

	// Define test events
	events := []store.EventRecord{
		store.NewEventRecord("test-stream-1", "TypeA", []byte(`{"field":"valueA"}`), []string{"test"}),
		store.NewEventRecord("test-stream-2", "TypeB", []byte(`{"field":"valueB"}`), []string{"example"}),
	}

	// Create subscriptions for subscription groups
	subscription, _ := es.Subscribe("subscription-group-1")
	subscription2, _ := es.Subscribe("subscription-group-2")

	assert.NotNil(t, subscription, "Subscription for subscription-group-1 should exist")
	assert.NotNil(t, subscription2, "Subscription for subscription-group-2 should exist")

	// Start a goroutine to listen to the first subscription
	receivedEvents := make(chan store.EventRecord, len(events))
	go func() {
		for event := range subscription.Listen() {
			receivedEvents <- event
		}
	}()

	// Start a goroutine to listen to the second subscription
	receivedEventsFor2 := make(chan store.EventRecord, len(events))
	go func() {
		for event := range subscription2.Listen() {
			receivedEventsFor2 <- event
		}
	}()

	// Append events
	for _, event := range events {
		_, err := es.AppendEvent(event)
		assert.NoError(t, err)
	}

	// Close subscriptions to stop listening
	subscription.Close()
	subscription2.Close()

	// Allow time for goroutines to process events
	time.Sleep(200 * time.Millisecond)

	// Verify the received events for the first subscription
	assert.Len(t, receivedEvents, len(events), "All events should be received by subscription 1")
	for _, expectedEvent := range events {
		receivedEvent := <-receivedEvents
		assert.Equal(t, expectedEvent.Stream, receivedEvent.Stream, "Stream mismatch")
		assert.Equal(t, expectedEvent.Type, receivedEvent.Type, "Type mismatch")
		assert.Equal(t, expectedEvent.Event, receivedEvent.Event, "Event data mismatch")
		assert.ElementsMatch(t, expectedEvent.Metadata.Tags, receivedEvent.Metadata.Tags, "Metadata tags mismatch")
	}

	// Verify the received events for the second subscription
	assert.Len(t, receivedEventsFor2, len(events), "All events should be received by subscription 2")
	for _, expectedEvent := range events {
		receivedEvent := <-receivedEventsFor2
		assert.Equal(t, expectedEvent.Stream, receivedEvent.Stream, "Stream mismatch")
		assert.Equal(t, expectedEvent.Type, receivedEvent.Type, "Type mismatch")
		assert.Equal(t, expectedEvent.Event, receivedEvent.Event, "Event data mismatch")
		assert.ElementsMatch(t, expectedEvent.Metadata.Tags, receivedEvent.Metadata.Tags, "Metadata tags mismatch")
	}
}
