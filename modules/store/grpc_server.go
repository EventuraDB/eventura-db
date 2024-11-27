package store

import (
	"context"
	"eventura/api/pb"
	"log"
	"time"
)

type EventStoreServer struct {
	store *EventStore
	pb.UnimplementedEventServiceServer
}

func NewEventStoreServer(store *EventStore) *EventStoreServer {
	return &EventStoreServer{store: store}
}

func (s *EventStoreServer) AppendEvent(ctx context.Context, req *pb.AppendEventRequest) (*pb.AppendEventResponse, error) {
	start := time.Now()
	log.Printf("Received AppendEvent request for Category: %s, EventType: %s", req.Category, req.EventType)

	// Create a new event with metadata
	event := NewEvent(req.Category, req.EventType, req.Payload)
	event.Metadata = Metadata{
		ContentType: req.Metadata.ContentType,
		Map:         req.Metadata.Map,
		Timestamp:   time.Now(),
	}

	storeStart := time.Now()
	id, err := s.store.StoreEvent(&event)
	if err != nil {
		log.Printf("AppendEvent failed for Category: %s, EventType: %s: %v", req.Category, req.EventType, err)
		return nil, err
	}
	log.Printf("StoreEvent completed for Category: %s, EventType: %s in %s", req.Category, req.EventType, time.Since(storeStart))

	log.Printf("AppendEvent request completed for Category: %s, EventID: %d in %s", req.Category, id, time.Since(start))
	return &pb.AppendEventResponse{EventId: id}, nil
}

func (s *EventStoreServer) ReadBySubject(ctx context.Context, req *pb.ReadBySubjectRequest) (*pb.ReadStreamResponse, error) {
	start := time.Now()
	log.Printf("Received ReadBySubject request for Subject: %s", req.Subject)

	// Fetch events from the store using the subject
	events, err := s.store.ReadBySubject(req.Subject, req.FromEventId)
	if err != nil {
		log.Printf("Failed to read events by subject (Subject: %s): %v", req.Subject, err)
		return nil, err
	}

	log.Printf("Fetched %d events for Subject: %s in %s", len(events), req.Subject, time.Since(start))

	// Convert events to Protobuf format
	var protoEvents []*pb.Event
	for _, event := range events {
		protoEvents = append(protoEvents, &pb.Event{
			Id:        event.ID,
			Category:  event.Category,
			Type:      event.Type,
			Payload:   event.Payload,
			Timestamp: event.Metadata.Timestamp.UnixNano(),
			Metadata: &pb.Metadata{
				ContentType: event.Metadata.ContentType,
				Map:         event.Metadata.Map,
			},
		})
	}

	log.Printf("Converted events to protobuf format for Subject: %s", req.Subject)
	return &pb.ReadStreamResponse{Events: protoEvents}, nil
}

/*func (s *EventStoreServer) SubscribeStream(request *pb.SubscribeEventTypeRequest, stream pb.EventService_SubscribeEventTypeServer) error {
log.Printf("Received SubscribeStream request (ConsumerID: %s)", request.ConsumerId)

// Validate request
if request.ConsumerId == "" {
	err := fmt.Errorf("consumer ID cannot be empty")
	log.Printf("Validation failed for SubscribeStream request: %v", err)
	return err
}

// Channel to handle events asynchronously
eventChan := make(chan *pb.Event, 100) // Buffered channel to avoid blocking

// Start the subscription in a separate goroutine
go func() {
	defer close(eventChan)
	err := s.store.SubscribeAll(stream.Context(), request.ConsumerId, func(event *Event) error {
		// Convert Event to pb.Event for gRPC
		pbEvent := &pb.Event{
			Id:        event.ID,
			Type:      event.Type,
			StreamId:  event.StreamID,
			Payload:   event.Payload,
			Timestamp: event.Metadata.Timestamp.Unix(),
		}

		select {
		case eventChan <- pbEvent: // Non-blocking send to the event channel
			return nil
		case <-stream.Context().Done(): // Stop if the client disconnects
			return fmt.Errorf("client disconnected")
		}
	})
	if err != nil {
		log.Printf("Failed to subscribe to _all stream (ConsumerID: %s): %v", request.ConsumerId, err)
	}
}()

// Stream events to the gRPC client
for {
	select {
	case pbEvent, ok := <-eventChan:
		if !ok {
			// Channel is closed, exit loop
			return nil
		}
		// Send the event to the client
		if err := stream.Send(&pb.SubscribeStreamResponse{
			Event: pbEvent,
		}); err != nil {
			log.Printf("Failed to send event to client (ConsumerID: %s): %v", request.ConsumerId, err)
			return err
		}
		log.Printf("Event sent to ConsumerID: %s, EventID: %d", request.ConsumerId, pbEvent.Id)
	case <-stream.Context().Done():
		// Handle client disconnection
		log.Printf("Client disconnected from SubscribeStream (ConsumerID: %s)", request.ConsumerId)
		return nil
	}
}*/
