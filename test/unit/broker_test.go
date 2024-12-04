package store

import (
	"eventura/modules/pubsub"
	"eventura/modules/pubsub/types"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"log"
	"sync"
	"testing"
	"time"
)

func TestPublishAndSubscribe(t *testing.T) {
	tempDir := t.TempDir()
	broker := pubsub.NewBroker(tempDir)

	// Track all active subscription goroutines
	subscriptionWg := &sync.WaitGroup{}

	// Close broker only after all subscriptions are done
	defer func() {
		subscriptionWg.Wait() // Ensure all subscriptions are finished
		assert.NoError(t, broker.Close())
	}()

	count := 0
	count2 := 0
	count3 := 0

	topic := broker.NewTopic("topic1")
	topic2 := broker.NewTopic("topic2")

	topic.Publish([]byte("test-message 1"))
	topic.Publish([]byte("test-message 2"))
	topic.Publish([]byte("test-message 3"))

	wg1 := &sync.WaitGroup{}
	wg2 := &sync.WaitGroup{}
	wg3 := &sync.WaitGroup{}

	wg1.Add(5)
	wg2.Add(5)
	wg3.Add(1)

	go func() {

		topic.Publish([]byte("test-message 4"))
		topic.Publish([]byte("test-message 5"))
		topic2.Publish([]byte("test-message topic2 message 1"))
		time.Sleep(2 * time.Second)
	}()

	// Add each subscription to the WaitGroup

	go func() {

		topic.Subscribe("consumer-1", func(msg types.Message) error {
			log.Printf("Received message: %s consumer %s", msg.Data, "consumer-1")
			count++
			defer wg1.Done()
			return nil
		})
	}()

	go func() {
		topic.Subscribe("consumer-2", func(msg types.Message) error {
			log.Printf("Received message: %s consumer %s", msg.Data, "consumer-2")
			time.Sleep(1 * time.Second)
			count2++
			defer wg2.Done()
			return nil
		})

		topic2.Subscribe("consumer-2", func(msg types.Message) error {
			log.Printf("Received message: %s consumer %s", msg.Data, "consumer-2")
			time.Sleep(1 * time.Second)
			count3++
			defer wg3.Done()
			return nil
		})

	}()

	wg1.Wait()
	wg2.Wait()
	wg3.Wait()

	time.Sleep(4 * time.Second)

	t.Run("TestPublishAndSubscribe", func(t *testing.T) {
		assert.Equal(t, 5, count)
		assert.Equal(t, 5, count2)
		assert.Equal(t, 1, count3)
	})

	dumpData(broker.Db)
}

func dumpData(db *pebble.DB) {
	iter, _ := db.NewIter(nil)
	defer iter.Close()

	fmt.Println("Dumping Pebble DB contents:")
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Print the key and value
		fmt.Printf("Key: %s, Value: %s\n", key, value)
	}

	if err := iter.Error(); err != nil {
		log.Fatalf("Iterator error: %v", err)
	}
}
