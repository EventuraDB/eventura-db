package store_test

import (
	"encoding/json"
	"eventura/modules/store"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendEventWithSubjectRead(t *testing.T) {
	// Setup test directory for Pebble
	testDir := filepath.Join(os.TempDir(), "test_event_store_subject")
	defer os.RemoveAll(testDir)

	// Initialize the EventStore
	eventStore, err := store.NewEventStore(testDir, nil)
	assert.NoError(t, err)
	defer eventStore.Close()

	err = eventStore.AddIndex(store.IndexConfig{
		IndexName: "by-order-id",
		EventType: "order-created",
		JSONPath:  "$.order.id",
	})
	assert.NoError(t, err)

	// Create a test event
	payload := map[string]interface{}{
		"order": map[string]interface{}{
			"id": "1234",
		},
	}
	payloadBytes, err := json.Marshal(payload)
	assert.NoError(t, err)

	event := store.Event{
		Category: "orders",
		Type:     "order-created",
		Payload:  payloadBytes,
		Metadata: store.Metadata{
			ContentType: "application/json",
			Map:         map[string]string{"key": "value"},
		},
	}

	// Call StoreEvent

	eventID, err := eventStore.StoreEvent(&event)
	assert.NoError(t, err)
	assert.NotZero(t, eventID)

	// Perform a read by subject
	subject := "by-order-id.*"
	fromEventID := uint64(0)
	events, err := eventStore.ReadBySubject(subject, fromEventID)
	assert.NoError(t, err)
	assert.NotEmpty(t, events, "Expected events for subject 1234 but found none")
	assert.Equal(t, 1, len(events), "Expected exactly one event")

	// Validate the stored event matches
	storedEvent := events[0]
	assert.Equal(t, eventID, storedEvent.ID)
	assert.Equal(t, "orders", storedEvent.Category)
	assert.Equal(t, "order-created", storedEvent.Type)

	// Check payload equality
	var storedPayload map[string]interface{}
	err = json.Unmarshal(storedEvent.Payload, &storedPayload)
	assert.NoError(t, err)
	assert.Equal(t, payload, storedPayload)
}
