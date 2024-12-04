package store

import (
	"eventura/modules/pubsub"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestBroker(t *testing.T) {
	tempDir := t.TempDir()
	broker := pubsub.NewBroker(tempDir)
	defer func() {
		assert.NoError(t, broker.Close())
	}()

	topic := "test-topic"
	messageReceived := make(chan pubsub.Message, 1)

	// Subscribe to the topic
	consumer := broker.Subscribe(topic, func(msg pubsub.Message) {
		messageReceived <- msg
	})
	defer broker.StopConsumer(consumer)

	// Publish a message
	now := time.Now().UnixNano()
	testMessage := pubsub.Message{
		ID:   uint64(now), // Match expected MessageID with generated one
		Data: []byte("test-message"),
	}
	broker.Publish(topic, testMessage)

	select {
	case msg := <-messageReceived:
		assert.Equal(t, testMessage.Data, msg.Data, "Message data mismatch")
	case <-time.After(2 * time.Second):
		t.Fatal("Message was not received by the subscription")
	}
}

func TestBrokerMultipleMessages(t *testing.T) {
	tempDir := t.TempDir()
	broker := pubsub.NewBroker(tempDir)
	defer func() {
		assert.NoError(t, broker.Close())
	}()

	topic := "multi-message-topic"
	messageReceived := make(chan pubsub.Message, 3)

	// Subscribe to the topic
	consumer := broker.Subscribe(topic, func(msg pubsub.Message) {
		messageReceived <- msg
	})
	defer broker.StopConsumer(consumer)

	// Publish multiple messages
	messages := []pubsub.Message{
		{Data: []byte("message-1")}, // Removed `MessageID` since it is auto-generated
		{Data: []byte("message-2")},
		{Data: []byte("message-3")},
	}

	for _, msg := range messages {
		broker.Publish(topic, msg)
	}

	for i := 0; i < len(messages); i++ {
		select {
		case msg := <-messageReceived:
			assert.Equal(t, messages[i].Data, msg.Data, "Message data mismatch")
		case <-time.After(2 * time.Second):
			t.Fatal("Message was not received by the subscription")
		}
	}
}

func TestBrokerConsumerStop(t *testing.T) {
	tempDir := t.TempDir()
	broker := pubsub.NewBroker(tempDir)
	defer func() {
		assert.NoError(t, broker.Close())
	}()

	topic := "stop-subscription-topic"
	messageReceived := make(chan pubsub.Message, 1)

	// Subscribe to the topic
	consumer := broker.Subscribe(topic, func(msg pubsub.Message) {
		messageReceived <- msg
	})

	// Stop the subscription
	broker.StopConsumer(consumer)

	// Publish a message
	broker.Publish(topic, pubsub.Message{
		ID:   1,
		Data: []byte("message-after-stop"),
	})

	select {
	case <-messageReceived:
		t.Fatal("Consumer received a message after being stopped")
	case <-time.After(1 * time.Second):
		// Pass test as subscription did not receive the message
	}
}

func TestBrokerParallelismWithPerformance(t *testing.T) {
	tempDir := t.TempDir()
	broker := pubsub.NewBroker(tempDir)
	defer func() {
		assert.NoError(t, broker.Close())
	}()

	topic := "parallel-topic"

	// Number of subscribers and messages
	numSubscribers := 2
	numMessages := 5

	// Channels to collect received messages for each subscriber
	messageChannels := make([]chan pubsub.Message, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		messageChannels[i] = make(chan pubsub.Message, numMessages)
	}

	// Map to store processing times for each subscriber
	processingTimes := make([][]time.Duration, numSubscribers)

	// Create multiple subscribers
	for i := 0; i < numSubscribers; i++ {
		subscriberID := i
		processingTimes[subscriberID] = []time.Duration{}

		broker.Subscribe(topic, func(msg pubsub.Message) {
			start := time.Now() // Record start time
			messageChannels[subscriberID] <- msg
			// Simulate processing time
			time.Sleep(time.Duration(rand.Intn(145)+5) * time.Millisecond)
			log.Printf("Subscriber %d processed message %s", subscriberID, string(msg.Data))
			end := time.Now() // Record end time

			// Calculate and store processing time
			processingTimes[subscriberID] = append(processingTimes[subscriberID], end.Sub(start))
		})
	}

	// Publish messages concurrently
	go func() {
		for i := 1; i <= numMessages; i++ {
			broker.Publish(topic, pubsub.Message{
				Data: []byte(fmt.Sprintf("message-%d", i)),
			})
		}
	}()

	// Verify that all subscribers receive all messages
	for i := 0; i < numSubscribers; i++ {
		subscriberID := i
		t.Run(fmt.Sprintf("Subscriber-%d", subscriberID), func(t *testing.T) {
			receivedMessages := make([]string, 0, numMessages)
			for j := 0; j < numMessages; j++ {
				select {
				case msg := <-messageChannels[subscriberID]:
					receivedMessages = append(receivedMessages, string(msg.Data))
				case <-time.After(2 * time.Second):
					t.Fatalf("Subscriber %d did not receive all messages", subscriberID)
				}
			}

			// Verify that all messages were received
			for j := 1; j <= numMessages; j++ {
				expected := fmt.Sprintf("message-%d", j)
				assert.Contains(t, receivedMessages, expected, "Missing message in subscriber")
			}
		})
	}

	// Log processing times for each subscriber
	for i := 0; i < numSubscribers; i++ {
		log.Printf("Subscriber %d processing times: %v", i, processingTimes[i])
	}

	// Analyze performance
	t.Run("Performance Analysis", func(t *testing.T) {
		for i := 0; i < numSubscribers; i++ {
			totalTime := time.Duration(0)
			for _, duration := range processingTimes[i] {
				totalTime += duration
			}
			averageTime := totalTime / time.Duration(len(processingTimes[i]))
			log.Printf("Subscriber %d average processing time: %s", i, averageTime)
			assert.LessOrEqual(t, averageTime.Milliseconds(), int64(200), "Subscriber %d processing is too slow", i)
		}
	})
}

func TestBrokerConsumerWithRetry(t *testing.T) {
	tempDir := t.TempDir()
	broker := pubsub.NewBroker(tempDir)
	defer func() {
		assert.NoError(t, broker.Close())
	}()

	t := broker.NewTopic("topic1")
	
}


topic := "retry-topic"
messageReceived := make(chan pubsub.Message, 10)

retryCount := 0
broker.Subscribe(topic, func (msg pubsub.Message) {
	if string(msg.Data) == "retry-message" && retryCount < 3 {
		retryCount++
		log.Printf("Simulating error for message: %s", msg.Data)
		panic("simulated transient error") // Simulate a temporary error
	}
	messageReceived <- msg
})

broker.Publish(topic, pubsub.Message{
ID:   1,
Data: []byte("normal-message"),
})

broker.Publish(topic, pubsub.Message{
ID:   2,
Data: []byte("retry-message"),
})

broker.Publish(topic, pubsub.Message{
ID:   3,
Data: []byte("final-message"),
})

receivedMessages := []string{}
for i := 0; i < 3; i++ {
select {
case msg := <-messageReceived:
receivedMessages = append(receivedMessages, string(msg.Data))
case <-time.After(7 * time.Second):
t.Fatal("Did not receive expected messages")
}
}

assert.Contains(t, receivedMessages, "normal-message")
assert.Contains(t, receivedMessages, "retry-message")
assert.Contains(t, receivedMessages, "final-message")
assert.Equal(t, 3, retryCount, "Expected 3 retries for retry-message")
}
