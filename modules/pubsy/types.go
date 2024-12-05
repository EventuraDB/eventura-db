package pubsy

import "time"

type TopicMessage struct {
	ID      uint64
	Data    []byte
	Topic   string
	Created time.Time
}

type SubscriptionStatus string

const (
	SubscriptionStatusNew      SubscriptionStatus = "new"
	SubscriptionStatusPending  SubscriptionStatus = "pending"
	SubscriptionStatusConsumed SubscriptionStatus = "consumed"
)

type SubscriptionMessage struct {
	Pointer  uint64 //pointer to the message in the topic
	Consumer string
	Topic    string
	Created  time.Time
	Status   SubscriptionStatus
}

type Message struct {
	ID uint64
}

type MessageHandler func(msg *Message) error

type SubscriptionInfo struct {
	Consumer string
	Topic    string
	Created  time.Time
	Offset   uint64 //offset of the last message consumed
}
