package pubsub

import (
	"eventura/modules/pubsub/subscription"
	"eventura/modules/pubsub/types"
)

func example() {

	broker := NewBroker("db/")

	topic := broker.NewTopic("topic1")

	topic.Publish(types.Message{
		ID:   1,
		Data: []byte("test-message"),
	})

	topic.Subscribe("consumer-1",
		subscription.SubscriptionOptions{
			RetryPolicy: 3,
			Backoff:     1,
		}, handle,
	)
}

func handle(msg types.Message) error {
	// Handle message
	return nil
}
