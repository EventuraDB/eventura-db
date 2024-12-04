package store

import (
	"go.uber.org/zap"
)

type Subscription struct {
	ConsumerGroup string
	offset        uint64 // Last acknowledged event MessageID todo how to handle overtakes ?! can they happen?
	eventsCh      chan EventRecord
	log           *zap.Logger
	stopCh        chan struct{}
}

func (s *Subscription) Notify(event *EventRecord) {
	select {
	case s.eventsCh <- *event:
		s.offset = event.ID
	default:
		s.log.Warn("Subscription buffer full, dropping event", zap.Uint64("event_id", event.ID))
	}
}

func (s *Subscription) Listen() <-chan EventRecord {
	return s.eventsCh
}

func (s *Subscription) Close() {
	close(s.eventsCh)
}

func (s *Subscription) UpdateOffset(eventID uint64) {
	s.offset = eventID
}

func (s *Subscription) IsStopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// CurrentOffset retrieves the current offset in a thread-safe manner.
func (s *Subscription) CurrentOffset() uint64 {
	return s.offset
}
