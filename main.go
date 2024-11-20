package main

import (
	"eventura/eventura"
	"eventura/eventura/pb"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	store, err := eventura.NewEventStore("db")
	if err != nil {
		log.Fatalf("Failed to create event store: %v", err)
	}
	defer store.Close()

	grpcServer := grpc.NewServer()
	eventuraGrpcServer := eventura.NewEventStoreServer(store)
	pb.RegisterEventStoreServiceServer(grpcServer, eventuraGrpcServer)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Starting gRPC server on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
