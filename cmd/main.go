package main

import (
	"eventura/api/pb"
	"eventura/modules/store"
	"github.com/nats-io/nats-server/v2/server"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

func main() {

	ns := setupNats()

	store, err := store.NewEventStore("db", ns)
	if err != nil {
		log.Fatalf("Failed to create event store: %v", err)
	}
	defer store.Close()

	grpcServer := grpc.NewServer()
	eventuraGrpcServer := store.NewEventStoreServer(store)
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
	waitForShutdown(grpcServer, ns)
}

func waitForShutdown(grpcServer *grpc.Server, ns *server.Server) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan
	log.Println("Shutting down gRPC server...")
	grpcServer.GracefulStop()

	log.Println("Shutting down NATS server...")
	ns.Shutdown()
	log.Println("Shutdown complete")
}

func setupNats() *server.Server {
	// STart embedded NATS server
	opts := &server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  "./tmp/nats",
	}
	natsServer, err := server.NewServer(opts)
	if err != nil {
		log.Fatalf("Error starting NATS server: %v", err)
	}
	go natsServer.Start()

	natsTime := time.Now()
	log.Printf("Starting NATS server on url %s", natsServer.ClientURL())
	if !natsServer.ReadyForConnections(60 * time.Second) {
		log.Fatalf("NATS server not ready for connections")
	}

	log.Printf("NATS server ready for connections after %d /n", time.Since(natsTime))

	return natsServer
}
