package store

import (
	"time"
)

type Metadata struct {
	Custom map[string]string
	Tags   []string
}

type EventRecord struct {
	ID        uint64    `json:"id"`
	Stream    string    `json:"stream"`
	Type      string    `json:"type"`
	Event     []byte    `json:"event"`
	Timestamp time.Time `json:"timestamp"`
	Metadata  Metadata
}

func NewEventRecord(stream, eventType string, event []byte, tags []string) EventRecord {
	return EventRecord{
		Stream:    stream,
		Type:      eventType,
		Event:     event,
		Timestamp: time.Now().UTC(),
		Metadata: Metadata{
			Custom: make(map[string]string),
			Tags:   tags,
		},
	}
}
