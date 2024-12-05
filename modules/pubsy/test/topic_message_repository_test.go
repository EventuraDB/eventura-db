package test

import (
	"eventura/modules/pubsy"
	"eventura/modules/utils"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
	"time"
)

func setupTestDb() (*pebble.DB, func()) {
	db, err := pebble.Open("temp/db", &pebble.Options{})
	if err != nil {
		panic(fmt.Sprintf("Failed to create test Pebble DB: %v", err))
	}
	return db, func() {
		db.Close()
		os.RemoveAll("temp/db")

	}
}

func setupDependency(db *pebble.DB) *pubsy.DbAwareDependency {
	logger, _ := zap.NewDevelopment()
	base := pubsy.NewBaseDependency(logger)
	dep := pubsy.NewDbAwareDependency(&base, db)
	return &dep
}

func TestGetLastMessageID(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	repo := pubsy.NewTopicMessageRepositoryImpl(setupDependency(db))
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

	repo := pubsy.NewTopicMessageRepositoryImpl(setupDependency(db))
	topic := "empty-topic"

	// Act
	lastID, _ := repo.GetLastMessageID(topic)

	// Assert
	assert.Equal(t, uint64(0), lastID)
}

func TestSave(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	repo := pubsy.NewTopicMessageRepositoryImpl(setupDependency(db))
	topic := "test-topic"

	// Act
	msg, err := repo.Save(topic, []byte("test-message"))

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, topic, msg.Topic)
	assert.Equal(t, []byte("test-message"), msg.Data)
	assert.Greater(t, msg.ID, uint64(0))

	// Verify the saved message using GetMessage
	storedMsg, err := repo.GetMessage(topic, msg.ID)
	assert.NoError(t, err)
	assert.NotNil(t, storedMsg)
	assert.Equal(t, msg.ID, storedMsg.ID)
	assert.Equal(t, msg.Topic, storedMsg.Topic)
	assert.Equal(t, msg.Data, storedMsg.Data)

}

func TestGetMessagesFromOffset(t *testing.T) {
	// Arrange
	db, cleanup := setupTestDb()
	defer cleanup()

	repo := pubsy.NewTopicMessageRepositoryImpl(setupDependency(db))
	topic := "test-topic"

	// Insert test messages
	start := time.Now()
	for i := 1; i <= 55; i++ {
		_, err := repo.Save(topic, []byte(fmt.Sprintf("message-%d", i)))
		assert.NoError(t, err)
	}
	fmt.Printf("Inserting 550 messages took: %v\n", time.Since(start))

	t.Run("GetMessagesFromOffset_InvalidOffset", func(t *testing.T) {
		startReading := time.Now()
		messages, err := repo.GetMessagesFromOffset(topic, 40, 55)
		fmt.Printf("Reading 55 messages took: %v\n", time.Since(startReading))

		// Assert
		assert.NoError(t, err)
		assert.NotEmpty(t, messages)
		assert.NotNil(t, messages)
		assert.Equal(t, 16, len(*messages))

	})

	dumpDb(db)

}

func TestDeleteMessagesOlderThan(t *testing.T) {
	db, cleanup := setupTestDb()
	defer cleanup()

	// Create the repository implementation
	repo := pubsy.NewTopicMessageRepositoryImpl(setupDependency(db))

	topic := "testTopic"

	// Insert first message (older)
	data1 := []byte("Older message")
	msg1, err := repo.Save(topic, data1)
	if err != nil {
		t.Fatalf("Failed to save first message: %v", err)
	}

	// Wait 1 second to ensure a time difference
	time.Sleep(1 * time.Second)

	// Insert second message (newer)
	data2 := []byte("Newer message")
	msg2, err := repo.Save(topic, data2)
	if err != nil {
		t.Fatalf("Failed to save second message: %v", err)
	}

	// Now, define a duration shorter than the difference between the two inserts.
	// The first message is older by about 1 second, so deleting older than 500ms should remove it.
	duration := 500 * time.Millisecond

	// Delete messages older than 'duration'.
	count, delErr := repo.DeleteMessagesOlderThan(topic, duration)
	if delErr != nil {
		t.Fatalf("DeleteMessagesOlderThan failed: %v", delErr)
	}

	assert.Equal(t, uint64(1), count, "Expected 1 message to be deleted")

	// Verify that the older message no longer exists
	_, closer, err := db.Get(utils.NewCompositeKey(pubsy.KEY_SPACE).
		AddString(topic).
		AddUint64(msg1.ID).
		Build())
	if closer != nil {
		closer.Close()
	}
	assert.Error(t, err, "Older message should have been deleted")

	// Verify that the newer message still exists
	val, closer, err := db.Get(utils.NewCompositeKey(pubsy.KEY_SPACE).
		AddString(topic).
		AddUint64(msg2.ID).
		Build())
	if err != nil {
		t.Fatalf("Expected newer message to still exist, got error: %v", err)
	}
	assert.Equal(t, data2, val, "Newer message should still be present and match original data")
	if closer != nil {
		closer.Close()
	}

	fmt.Println("TestDeleteMessagesOlderThan passed successfully.")
}

// dumpDb prints all keys and values from the provided Pebble database.
func dumpDb(db *pebble.DB) {
	iter, _ := db.NewIter(nil)
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("Database dump - Key: %s, Value: %s\n", key, value)
	}
}
