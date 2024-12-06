package test

import (
	"encoding/json"
	"eventura/modules/pubsy/core"
	"eventura/modules/pubsy/internal"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
	"time"
)

func setupDependency(db *pebble.DB) (*pebble.DB, *zap.Logger) {
	logger, _ := zap.NewDevelopment()
	return db, logger
}

func TestGetLastMessageID(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	repo := internal.NewTopicRepositoryImpl(setupDependency(db))
	topic := "test-topic"

	// Insert test data
	for i := 1; i <= 5; i++ {
		_, err := repo.Save(topic, []byte(fmt.Sprintf("message-%d", i)))
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Act
	lastID, err := repo.GetLastMessageID(topic)

	// Assert
	assert.NoError(t, err)

	assert.Equal(t, uint64(5), lastID)

}

func TestGetLastMessageID_NoMessages(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	repo := internal.NewTopicRepositoryImpl(setupDependency(db))
	topic := "empty-topic"

	// Act
	lastID, _ := repo.GetLastMessageID(topic)

	// Assert
	assert.Equal(t, uint64(0), lastID)
}

func TestSave(t *testing.T) {
	db, err := pebble.Open("testdb", &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	logger, _ := zap.NewDevelopment()
	repo := internal.NewTopicRepositoryImpl(db, logger)

	topic := "test-topic"
	data := []byte("test-message")

	topicMessage, err := repo.Save(topic, data)
	require.NoError(t, err)

	// Construct the expected TopicMessage
	expectedMessage := &core.TopicMessage{
		ID:      topicMessage.ID, // Use returned ID to ensure consistency
		Data:    data,
		Topic:   topic,
		Created: topicMessage.Created, // Use returned timestamp
	}

	expectedJSON, err := json.Marshal(expectedMessage)
	require.NoError(t, err)

	// Retrieve the stored data
	key := utils.NewCompositeKey(internal.KEY_SPACE).
		AddString(topic).
		AddUint64(topicMessage.ID).
		Build()

	storedData, closer, err := db.Get(key)
	require.NoError(t, err)
	defer closer.Close()

	// Assert stored data matches expected JSON
	assert.Equal(t, expectedJSON, storedData)
}

func TestGetMessagesFromOffset(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	repo := internal.NewTopicRepositoryImpl(setupDependency(db))
	topic := "test-topic"

	// Insert test messages
	start := time.Now()
	for i := 1; i <= 55; i++ {
		_, err := repo.Save(topic, []byte(fmt.Sprintf("message-%d", i)))
		require.NoError(t, err, "Failed to save message %d", i)
	}
	fmt.Printf("Inserting 55 messages took: %v\n", time.Since(start))

	t.Run("GetMessagesFromOffset_ValidOffset", func(t *testing.T) {
		startReading := time.Now()
		messages, err := repo.GetMessagesFromOffset(topic, 40, 15)
		fmt.Printf("Reading 15 messages took: %v\n", time.Since(startReading))

		// Assert
		require.NoError(t, err, "GetMessagesFromOffset should not return an error")
		require.NotNil(t, messages, "Messages should not be nil")
		assert.Equal(t, 15, len(*messages), "Expected to fetch 15 messages starting from offset 40")

		// Adjust expectations for inclusive offset
		for i, msg := range *messages {
			expectedMessage := fmt.Sprintf("message-%d", 40+i)
			assert.Equal(t, expectedMessage, string(msg.Data), "Message data does not match")
			assert.Equal(t, topic, msg.Topic, "Message topic does not match")
		}
	})

	t.Run("GetMessagesFromOffset_InvalidOffset", func(t *testing.T) {
		startReading := time.Now()
		messages, err := repo.GetMessagesFromOffset(topic, 100, 10)
		fmt.Printf("Reading messages with invalid offset took: %v\n", time.Since(startReading))

		// Assert
		require.NoError(t, err, "GetMessagesFromOffset should not return an error")
		require.NotNil(t, messages, "Messages should not be nil")
		assert.Equal(t, 0, len(*messages), "Expected no messages to be returned for offset 100")
	})

	dumpDb(db)
}

func TestDeleteMessagesOlderThan(t *testing.T) {
	// Setup test database and cleanup
	db, err := pebble.Open("testdb", &pebble.Options{})
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	logger, _ := zap.NewDevelopment()
	repo := internal.NewTopicRepositoryImpl(db, logger)

	topic := "testTopic"

	// Insert the first message (older)
	data1 := []byte("Older message")
	msg1, err := repo.Save(topic, data1)
	require.NoError(t, err, "Failed to save first message")

	// Wait 1 second to ensure a time difference
	time.Sleep(1 * time.Second)

	// Insert the second message (newer)
	data2 := []byte("Newer message")
	msg2, err := repo.Save(topic, data2)
	require.NoError(t, err, "Failed to save second message")

	// Define a duration shorter than the time difference between the two inserts
	duration := 500 * time.Millisecond

	// Delete messages older than the specified duration
	count, delErr := repo.DeleteMessagesOlderThan(topic, duration)
	require.NoError(t, delErr, "DeleteMessagesOlderThan failed")

	// Validate the deletion count
	assert.Equal(t, uint64(1), count, "Expected 1 message to be deleted")

	// Verify that the older message no longer exists
	_, closer, err := db.Get(utils.NewCompositeKey(internal.KEY_SPACE).
		AddString(topic).
		AddUint64(msg1.ID).
		Build())
	if closer != nil {
		_ = closer.Close()
	}
	assert.Error(t, err, "Older message should have been deleted")

	// Verify that the newer message still exists
	val, closer, err := db.Get(utils.NewCompositeKey(internal.KEY_SPACE).
		AddString(topic).
		AddUint64(msg2.ID).
		Build())
	if err != nil {
		t.Fatalf("Expected newer message to still exist, got error: %v", err)
	}
	assert.NotNil(t, val, "Expected value for newer message to exist")
	assert.JSONEq(t, string(data2), string(val), "Newer message should still be present and match original data")
	if closer != nil {
		_ = closer.Close()
	}
}
