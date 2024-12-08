// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v3.20.3
// source: eventura.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	EventStoreService_AppendEvent_FullMethodName = "/eventura.EventStoreService/AppendEvent"
	EventStoreService_ReadStream_FullMethodName  = "/eventura.EventStoreService/ReadStream"
)

// EventStoreServiceClient is the client API for EventStoreService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type EventStoreServiceClient interface {
	AppendEvent(ctx context.Context, in *AppendEventRequest, opts ...grpc.CallOption) (*AppendEventResponse, error)
	ReadStream(ctx context.Context, in *ReadStreamRequest, opts ...grpc.CallOption) (*ReadStreamResponse, error)
}

type eventStoreServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewEventStoreServiceClient(cc grpc.ClientConnInterface) EventStoreServiceClient {
	return &eventStoreServiceClient{cc}
}

func (c *eventStoreServiceClient) AppendEvent(ctx context.Context, in *AppendEventRequest, opts ...grpc.CallOption) (*AppendEventResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AppendEventResponse)
	err := c.cc.Invoke(ctx, EventStoreService_AppendEvent_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eventStoreServiceClient) ReadStream(ctx context.Context, in *ReadStreamRequest, opts ...grpc.CallOption) (*ReadStreamResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReadStreamResponse)
	err := c.cc.Invoke(ctx, EventStoreService_ReadStream_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EventStoreServiceServer is the server API for EventStoreService service.
// All implementations must embed UnimplementedEventStoreServiceServer
// for forward compatibility.
type EventStoreServiceServer interface {
	AppendEvent(context.Context, *AppendEventRequest) (*AppendEventResponse, error)
	ReadStream(context.Context, *ReadStreamRequest) (*ReadStreamResponse, error)
	mustEmbedUnimplementedEventStoreServiceServer()
}

// UnimplementedEventStoreServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedEventStoreServiceServer struct{}

func (UnimplementedEventStoreServiceServer) AppendEvent(context.Context, *AppendEventRequest) (*AppendEventResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEvent not implemented")
}
func (UnimplementedEventStoreServiceServer) ReadStream(context.Context, *ReadStreamRequest) (*ReadStreamResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadStream not implemented")
}
func (UnimplementedEventStoreServiceServer) mustEmbedUnimplementedEventStoreServiceServer() {}
func (UnimplementedEventStoreServiceServer) testEmbeddedByValue()                           {}

// UnsafeEventStoreServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to EventStoreServiceServer will
// result in compilation errors.
type UnsafeEventStoreServiceServer interface {
	mustEmbedUnimplementedEventStoreServiceServer()
}

func RegisterEventStoreServiceServer(s grpc.ServiceRegistrar, srv EventStoreServiceServer) {
	// If the following call pancis, it indicates UnimplementedEventStoreServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&EventStoreService_ServiceDesc, srv)
}

func _EventStoreService_AppendEvent_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEventRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EventStoreServiceServer).AppendEvent(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: EventStoreService_AppendEvent_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EventStoreServiceServer).AppendEvent(ctx, req.(*AppendEventRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _EventStoreService_ReadStream_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadStreamRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EventStoreServiceServer).ReadStream(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: EventStoreService_ReadStream_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EventStoreServiceServer).ReadStream(ctx, req.(*ReadStreamRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// EventStoreService_ServiceDesc is the grpc.ServiceDesc for EventStoreService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var EventStoreService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "eventura.EventStoreService",
	HandlerType: (*EventStoreServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AppendEvent",
			Handler:    _EventStoreService_AppendEvent_Handler,
		},
		{
			MethodName: "ReadStream",
			Handler:    _EventStoreService_ReadStream_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "eventura.proto",
}
