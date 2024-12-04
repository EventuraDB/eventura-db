package main

import (
	"eventura/api/pb"
	"eventura/modules/store"
	"eventura/modules/store/pebble"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	eventStore, err := pebble.NewPebbleEventStore("db", logger)
	if err != nil {
		log.Fatalf("Failed to create event eventStore: %v", err)
	}
	defer eventStore.Close()

	grpcServer := grpc.NewServer()
	eventuraGrpcServer := store.NewEventStoreServer(eventStore, logger)
	pb.RegisterEventServiceServer(grpcServer, eventuraGrpcServer)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		log.Println("Starting gRPC server on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
		log.Println("gRPC server is up and running on :50051") // Will not execute if there's an error
	}()
	// Graceful shutdown
	waitForShutdown(grpcServer)
}

func waitForShutdown(grpcServer *grpc.Server) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan
	log.Println("Shutting down gRPC server...")
	grpcServer.GracefulStop()

	log.Println("Shutdown complete")
}
