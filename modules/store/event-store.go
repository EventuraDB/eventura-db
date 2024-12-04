package store

type EventStore interface {
	AppendEvent(event EventRecord) (uint64, error)
	ReadByTag(tag string, fromId uint64) (*[]EventRecord, error)
	ReadByStream(stream string, fromId uint64) (*[]EventRecord, error)
	ReadByEventType(eventType string, fromId uint64) (*[]EventRecord, error)
	ReadAll() ([]EventRecord, error)
	GetEventByID(id uint64) (*EventRecord, error)
	Close() error
	Subscribe(consumerId string) (*Subscription, error)
	Acknowledge(consumerId string, eventID uint64) error
	GetSubscriptions() map[string]*Subscription
}
