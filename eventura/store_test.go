package eventura

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
)

// TestEventStore is the test suite for EventStore.
func TestEventStore(t *testing.T) {
	// Setup in-memory Badger instance for testing
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	assert.NoError(t, err)
	defer db.Close()

	eventStore := &EventStore{db: db}

	// Sample events for testing
	event1 := Event{
		ID:        "event1",
		StreamID:  "stream1",
		Payload:   []byte(`{"key": "value1"}`),
		Timestamp: time.Now(),
	}

	event2 := Event{
		ID:        "event2",
		StreamID:  "stream1",
		Payload:   []byte(`{"key": "value2"}`),
		Timestamp: time.Now().Add(1 * time.Second),
	}

	ctx := context.Background()

	// Test StoreEvent
	t.Run("StoreEvent", func(t *testing.T) {
		_, err := eventStore.StoreEvent(ctx, event1)
		assert.NoError(t, err)

		_, err = eventStore.StoreEvent(ctx, event2)
		assert.NoError(t, err)
	})

	// Test ReadStream
	t.Run("ReadStream", func(t *testing.T) {
		events, err := eventStore.ReadStream(ctx, "stream1", 0, 10)
		assert.NoError(t, err)
		assert.Len(t, events, 2)

		assert.Equal(t, "event1", events[0].ID)
		assert.Equal(t, "event2", events[1].ID)
		assert.Equal(t, `{"key": "value1"}`, string(events[0].Payload))
		assert.Equal(t, `{"key": "value2"}`, string(events[1].Payload))
	})

	// Test ReadAll with resolveData = true
	t.Run("ReadAll with resolveData=true", func(t *testing.T) {
		events, err := eventStore.ReadAll(ctx, 0, 10, true)
		assert.NoError(t, err)
		assert.Len(t, events, 2)

		assert.Equal(t, "event1", events[0].ID)
		assert.Equal(t, "event2", events[1].ID)
		assert.Equal(t, `{"key": "value1"}`, string(events[0].Payload))
		assert.Equal(t, `{"key": "value2"}`, string(events[1].Payload))
	})

	// Test ReadAll with resolveData = false
	t.Run("ReadAll with resolveData=false", func(t *testing.T) {
		events, err := eventStore.ReadAll(ctx, 0, 10, false)
		assert.NoError(t, err)
		assert.Len(t, events, 2)

		// Assert against full event keys
		expectedKeys := []string{
			generateEventStreamKey(event1.StreamID, event1.Timestamp, event1.ID),
			generateEventStreamKey(event2.StreamID, event2.Timestamp, event2.ID),
		}
		assert.Equal(t, expectedKeys[0], events[0].ID)
		assert.Equal(t, expectedKeys[1], events[1].ID)
	})
}
