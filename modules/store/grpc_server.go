package store

import (
	"context"
	"eventura/api/pb"
	"fmt"
	"go.uber.org/zap"
	"log"
	"time"
)

type EventStoreServer struct {
	store EventStore
	pb.UnimplementedEventServiceServer
	log *zap.Logger
}

func NewEventStoreServer(store EventStore, logger *zap.Logger) *EventStoreServer {
	return &EventStoreServer{store: store, log: logger}
}

func (s *EventStoreServer) AppendEvent(ctx context.Context, req *pb.AppendEventRequest) (*pb.AppendEventResponse, error) {
	start := time.Now()
	log.Printf("Received AppendEvent request for Stream: %s, EventType: %s", req.Stream, req.EventType)

	// Create a new event with metadata
	event := NewEventRecord(req.Stream, req.EventType, req.Payload, []string{})

	/*	event.Metadata = Metadata{
		Custom: req.Metadata.Custom,
	}*/

	//TODO move to NewEventRecord
	if req.Metadata != nil && req.Metadata.Tags != nil {
		event.Metadata = Metadata{
			Tags: req.Metadata.Tags,
		}
	}

	storeStart := time.Now()
	id, err := s.store.AppendEvent(event)
	if err != nil {
		log.Printf("AppendEvent failed for Stream: %s, EventType: %s: %v", req.Stream, req.EventType, err)
		return nil, err
	}
	log.Printf("AppendEvent completed for Stream: %s, EventType: %s in %s", req.Stream, req.EventType, time.Since(storeStart))

	log.Printf("AppendEvent request completed for Stream: %s, EventID: %d in %s", req.Stream, id, time.Since(start))
	return &pb.AppendEventResponse{EventId: id}, nil
}

func (s *EventStoreServer) ReadByStream(ctx context.Context, req *pb.ReadByStreamRequest) (*pb.ReadStreamResponse, error) {
	start := time.Now()
	log.Printf("Received ReadByStream request for Stream: %s", req.Stream)

	// Fetch events from the store using the stream
	events, err := s.store.ReadByStream(req.Stream, 0)
	if err != nil {
		log.Printf("Failed to read events by stream (Stream: %s): %v", req.Stream, err)
		return nil, err
	}

	log.Printf("Fetched %d events for Stream: %s in %s", len(*events), req.Stream, time.Since(start))

	// Convert events to Protobuf format
	var protoEvents []*pb.Event
	for _, event := range *events {
		protoEvents = append(protoEvents, mapToProto(event))
	}

	log.Printf("Converted events to protobuf format for Stream: %s", req.Stream)
	return &pb.ReadStreamResponse{Events: protoEvents}, nil
}

func (s *EventStoreServer) ReadByTags(ctx context.Context, req *pb.ReadByTagsRequest) (*pb.ReadStreamResponse, error) {
	start := time.Now()
	log.Printf("Received Read By Tags request for Subject: %s", req.Tag)

	// Fetch events from the store using the subject
	events, err := s.store.ReadByTag(req.Tag, req.FromId)
	if err != nil {
		log.Printf("Failed to read events by subject (Subject: %s): %v", req.Tag, err)
		return nil, err
	}

	log.Printf("Fetched %d events for Subject: %s in %s", len(*events), req.Tag, time.Since(start))

	// Convert events to Protobuf format
	var protoEvents []*pb.Event
	for _, event := range *events {
		protoEvents = append(protoEvents, mapToProto(event))
	}

	log.Printf("Converted events to protobuf format for Subject: %s", req.Tag)
	return &pb.ReadStreamResponse{Events: protoEvents}, nil
}

func (s *EventStoreServer) Subscribe(req *pb.SubscribeRequest, stream pb.EventService_SubscribeServer) error {
	log.Printf("Received Subscribe request (ConsumerID: %s)", req.ConsumerGroup)

	// Validate request
	if req.ConsumerGroup == "" {
		err := fmt.Errorf("subscription MessageID cannot be empty")
		log.Printf("Validation failed for Subscribe request: %v", err)
		return err
	}

	err := stream.Send(&pb.SubscribeResponse{Event: &pb.Event{}})
	if err != nil {
		log.Printf("Failed to send empty response to client (ConsumerID: %s): %v", req.ConsumerGroup, err)
		return err
	}

	return nil

}

func mapToProto(event EventRecord) *pb.Event {
	return &pb.Event{
		Id:        event.ID,
		Stream:    event.Stream,
		Type:      event.Type,
		Payload:   event.Event,
		Timestamp: event.Timestamp.Unix(),
		Metadata: &pb.Metadata{
			Custom: event.Metadata.Custom,
		},
	}
}

/*func (s *EventStoreServer) SubscribeStream(request *pb.SubscribeEventTypeRequest, stream pb.EventService_SubscribeEventTypeServer) error {
log.Printf("Received SubscribeStream request (ConsumerID: %s)", request.ConsumerId)

// Validate request
if request.ConsumerId == "" {
	err := fmt.Errorf("subscription MessageID cannot be empty")
	log.Printf("Validation failed for SubscribeStream request: %v", err)
	return err
}

// Channel to handle events asynchronously
eventChan := make(chan *pb.EventRecord, 100) // Buffered channel to avoid blocking

// Start the subscription in a separate goroutine
go func() {
	defer close(eventChan)
	err := s.store.SubscribeAll(stream.Context(), request.ConsumerId, func(event *EventRecord) error {
		// Convert EventRecord to pb.EventRecord for gRPC
		pbEvent := &pb.EventRecord{
			Id:        event.MessageID,
			Type:      event.Type,
			StreamId:  event.StreamID,
			Event:   event.Event,
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
			EventRecord: pbEvent,
		}); err != nil {
			log.Printf("Failed to send event to client (ConsumerID: %s): %v", request.ConsumerId, err)
			return err
		}
		log.Printf("EventRecord sent to ConsumerID: %s, EventID: %d", request.ConsumerId, pbEvent.Id)
	case <-stream.Context().Done():
		// Handle client disconnection
		log.Printf("Client disconnected from SubscribeStream (ConsumerID: %s)", request.ConsumerId)
		return nil
	}
}*/
