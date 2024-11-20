package eventura

import (
	_ "encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        string    `json:"id"`
	StreamID  string    `json:"stream_id"`
	Type      string    `json:"type"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

func NewEvent(streamID, eventType string, payload []byte) (*Event, error) {
	return &Event{
		ID:        uuid.New().String(),
		StreamID:  streamID,
		Type:      eventType,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}, nil
}
