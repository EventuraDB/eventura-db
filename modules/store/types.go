package store

import (
	_ "encoding/json"
	"time"
)

type Metadata struct {
	Timestamp   time.Time `json:"timestamp"`
	ContentType string    `json:"content_type"`
	Map         map[string]string
}

type Event struct {
	ID       uint64 `json:"id"`
	Category string `json:"category"`
	Type     string `json:"type"`
	Payload  []byte `json:"payload"`
	Metadata Metadata
}

func NewEvent(category, eventType string, payload []byte) Event {
	return Event{
		Category: category,
		Type:     eventType,
		Payload:  payload,
		Metadata: Metadata{
			Timestamp: time.Now().UTC(),
			Map:       make(map[string]string),
		},
	}
}

type IndexConfig struct {
	IndexName string
	EventType string
	JSONPath  string
}

type IndexDefinition struct {
	JSONPath string `json:"json_path"`
}
