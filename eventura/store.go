package eventura

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"log"
	"strings"
	"time"
)

const (
	Delimiter                     = ":"
	AllStreamKey                  = "_all"
	AllStreamKeyAndDelimiter      = AllStreamKey + Delimiter
	StreamRegistryKey             = "_stream-registry"
	StreamRegistryKeyAndDelimiter = StreamRegistryKey + Delimiter
	LinkPrefix                    = "#link"
)

type EventStore struct {
	db *badger.DB
}

func NewEventStore(path string) (*EventStore, error) {
	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &EventStore{db: db}, nil
}

func (es *EventStore) StoreEvent(ctx context.Context, event Event) (string, error) {
	var eventKey string

	err := es.db.Update(func(txn *badger.Txn) error {
		eventKey = generateEventStreamKey(event.StreamID, event.Timestamp, event.ID)
		eventData, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to serialize event: %w", err)
		}

		if err := txn.Set([]byte(eventKey), eventData); err != nil {
			return fmt.Errorf("failed to write event to stream: %w", err)
		}

		allKey := generateAllStreamKey()
		if err := txn.Set([]byte(allKey), []byte(generateLink(eventKey))); err != nil {
			return fmt.Errorf("failed to write event to _all stream: %w", err)
		}

		return nil
	})

	err = es.RegisterStream(event.StreamID)
	if err != nil {
		return "", err
	}

	if err != nil {
		return "", err
	}

	return eventKey, nil
}

func (es *EventStore) RegisterStream(streamID string) error {
	return es.db.Update(func(txn *badger.Txn) error {
		streamKey := []byte(StreamRegistryKeyAndDelimiter + streamID)

		// Check if the stream already exists
		_, err := txn.Get(streamKey)
		if err == nil {
			// Stream already exists, no need to register
			return nil
		} else if err != badger.ErrKeyNotFound {
			// If the error is not "key not found", return the error
			return fmt.Errorf("failed to check stream existence: %w", err)
		}
		payload := map[string]string{"stream_name": streamID}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to serialize payload: %w", err)
		}
		registerEvent, _ := NewEvent(string(streamKey), "stream-registered", payloadBytes)
		// Register the new stream

		eventData, err := json.Marshal(registerEvent)
		if err != nil {
			return fmt.Errorf("failed to serialize registered event: %w", err)
		}
		err = txn.Set([]byte(streamKey), []byte(eventData))
		if err != nil {
			return fmt.Errorf("failed to write registered event to stream: %w", err)
		}

		if err != nil {
			return fmt.Errorf("failed to register stream: %w", err)
		}
		return nil
	})
}

// Add link# prefix to identify a link to an event
func generateLink(key string) string {
	builder := strings.Builder{}
	builder.WriteString(LinkPrefix)
	builder.WriteString(key)

	return builder.String()
}

func (es *EventStore) ReadStream(ctx context.Context, streamID string, offset, limit int) ([]Event, error) {
	var events []Event

	err := es.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(streamID + Delimiter)
		it := txn.NewIterator(opts)
		defer it.Close()

		skipped, count := 0, 0
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			if skipped < offset {
				skipped++
				continue
			}
			if count >= limit {
				break
			}

			item := it.Item()
			eventData, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy event data: %w", err)
			}

			if bytes.HasPrefix(eventData, []byte(LinkPrefix)) {
				linkKey := eventData[len(LinkPrefix):]
				resolvedEvent, err := txn.Get(linkKey)
				if err != nil {
					return fmt.Errorf("failed to resolve linked event: %w", err)
				}
				eventData, err = resolvedEvent.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("failed to copy linked event data: %w", err)
				}
			}

			var event Event
			if err := json.Unmarshal(eventData, &event); err != nil {
				return fmt.Errorf("failed to deserialize event data: %w", err)
			}

			events = append(events, event)
			count++
		}
		return nil
	})
	if err != nil {
		log.Printf("Error reading stream %s: %v", streamID, err)
	}
	return events, err
}

func (es *EventStore) Close() error {
	return es.db.Close()
}

func generateEventStreamKey(streamID string, timestamp time.Time, eventID string) string {
	return fmt.Sprintf("%s:%s:%s", streamID, timestamp.Format("20060102150405"), eventID)
}

func generateAllStreamKey() string {
	return fmt.Sprintf("%s%d", AllStreamKeyAndDelimiter, time.Now().UnixNano())
}
