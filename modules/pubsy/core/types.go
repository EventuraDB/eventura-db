package core

import (
	"github.com/google/uuid"
	"time"
)

type TopicMessage struct {
	ID      uint64    `json:"id"`
	Data    []byte    `json:"data"`
	Topic   string    `json:"topic"`
	Created time.Time `json:"created"`
}

type SubscriptionStatus string

const (
	SubscriptionStatusNew      SubscriptionStatus = "new"
	SubscriptionStatusPending  SubscriptionStatus = "pending"
	SubscriptionStatusConsumed SubscriptionStatus = "consumed"
	SubscriptionStatusRetry    SubscriptionStatus = "retry"
	SubscriptionStatusFailed   SubscriptionStatus = "failed"
)

type SubscriptionMessage struct {
	Pointer  uint64 //pointer to the message in the topic
	Consumer string
	Topic    string
	Created  time.Time
	Status   SubscriptionStatus
}

type Message struct {
	ID   uint64
	Data []byte
}

type MessageHandler func(msg *Message) error

type SubscriptionInfo struct {
	ID                 uuid.UUID
	Consumer           string
	Topic              string
	Created            time.Time
	LastFetchedOffset  uint64 // offset of the latest message retrieved from the topic
	LastConsumedOffset uint64 // offset of the latest message fully processed by the consumer
}
