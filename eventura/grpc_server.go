package eventura

import (
	"context"
	"eventura/eventura/pb"
	"log"
	"sort"
)

type EventStoreServer struct {
	pb.UnimplementedEventStoreServiceServer
	store *EventStore
}

func NewEventStoreServer(store *EventStore) *EventStoreServer {
	return &EventStoreServer{store: store}
}

func (s *EventStoreServer) AppendEvent(ctx context.Context, req *pb.AppendEventRequest) (*pb.AppendEventResponse, error) {
	// Create a new event
	event, err := NewEvent(req.StreamId, req.EventType, req.Payload)
	if err != nil {
		log.Printf("Failed to create event: %v", err)
		return nil, err
	}

	if _, err := s.store.StoreEvent(ctx, *event); err != nil {
		log.Printf("Failed to append event: %v", err)
		return nil, err
	}

	return &pb.AppendEventResponse{EventId: event.ID}, nil
}

func (s *EventStoreServer) ReadStream(ctx context.Context, req *pb.ReadStreamRequest) (*pb.ReadStreamResponse, error) {
	// Fetch events from the store
	events, err := s.store.ReadStream(ctx, req.StreamId, int(req.Offset), int(req.Limit))
	if err != nil {
		log.Printf("Failed to read stream: %v", err)
		return nil, err
	}

	// Sort events by timestamp in ascending order
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	// Convert events to Protobuf format
	var protoEvents []*pb.Event
	for _, event := range events {
		protoEvents = append(protoEvents, &pb.Event{
			Id:        event.ID,
			StreamId:  event.StreamID,
			Type:      event.Type,
			Payload:   event.Payload,
			Timestamp: event.Timestamp.Unix(), // Convert to Unix timestamp
		})
	}

	return &pb.ReadStreamResponse{Events: protoEvents}, nil
}
