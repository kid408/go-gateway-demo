package sessionrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerServiceClient interface {
	ProcessSessionEvent(ctx context.Context, in *SessionEvent, opts ...grpc.CallOption) (*SessionResult, error)
}

type workerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWorkerServiceClient(cc grpc.ClientConnInterface) WorkerServiceClient {
	return &workerServiceClient{cc: cc}
}

func (c *workerServiceClient) ProcessSessionEvent(ctx context.Context, in *SessionEvent, opts ...grpc.CallOption) (*SessionResult, error) {
	out := new(SessionResult)
	err := c.cc.Invoke(ctx, "/sessionrpc.WorkerService/ProcessSessionEvent", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type WorkerServiceServer interface {
	ProcessSessionEvent(context.Context, *SessionEvent) (*SessionResult, error)
	mustEmbedUnimplementedWorkerServiceServer()
}

type UnimplementedWorkerServiceServer struct{}

func (UnimplementedWorkerServiceServer) ProcessSessionEvent(context.Context, *SessionEvent) (*SessionResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ProcessSessionEvent not implemented")
}

func (UnimplementedWorkerServiceServer) mustEmbedUnimplementedWorkerServiceServer() {}

type UnsafeWorkerServiceServer interface {
	mustEmbedUnimplementedWorkerServiceServer()
}

func RegisterWorkerServiceServer(s grpc.ServiceRegistrar, srv WorkerServiceServer) {
	s.RegisterService(&WorkerService_ServiceDesc, srv)
}

func _WorkerService_ProcessSessionEvent_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SessionEvent)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).ProcessSessionEvent(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/sessionrpc.WorkerService/ProcessSessionEvent",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(WorkerServiceServer).ProcessSessionEvent(ctx, req.(*SessionEvent))
	}
	return interceptor(ctx, in, info, handler)
}

var WorkerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "sessionrpc.WorkerService",
	HandlerType: (*WorkerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ProcessSessionEvent",
			Handler:    _WorkerService_ProcessSessionEvent_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "sessionrpc.json",
}
