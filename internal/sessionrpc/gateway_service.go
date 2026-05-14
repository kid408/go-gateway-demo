package sessionrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GatewayServiceClient interface {
	OpenSession(ctx context.Context, opts ...grpc.CallOption) (GatewayService_OpenSessionClient, error)
	ReportWorkerStatus(ctx context.Context, in *WorkerStatusReport, opts ...grpc.CallOption) (*WorkerStatusAck, error)
}

type gatewayServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewGatewayServiceClient(cc grpc.ClientConnInterface) GatewayServiceClient {
	return &gatewayServiceClient{cc: cc}
}

func (c *gatewayServiceClient) OpenSession(ctx context.Context, opts ...grpc.CallOption) (GatewayService_OpenSessionClient, error) {
	stream, err := c.cc.NewStream(ctx, &GatewayService_ServiceDesc.Streams[0], "/sessionrpc.GatewayService/OpenSession", opts...)
	if err != nil {
		return nil, err
	}
	return &gatewayServiceOpenSessionClient{ClientStream: stream}, nil
}

func (c *gatewayServiceClient) ReportWorkerStatus(ctx context.Context, in *WorkerStatusReport, opts ...grpc.CallOption) (*WorkerStatusAck, error) {
	out := new(WorkerStatusAck)
	err := c.cc.Invoke(ctx, "/sessionrpc.GatewayService/ReportWorkerStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

type GatewayServiceServer interface {
	OpenSession(GatewayService_OpenSessionServer) error
	ReportWorkerStatus(context.Context, *WorkerStatusReport) (*WorkerStatusAck, error)
	mustEmbedUnimplementedGatewayServiceServer()
}

type UnimplementedGatewayServiceServer struct{}

func (UnimplementedGatewayServiceServer) OpenSession(GatewayService_OpenSessionServer) error {
	return status.Errorf(codes.Unimplemented, "method OpenSession not implemented")
}

func (UnimplementedGatewayServiceServer) ReportWorkerStatus(context.Context, *WorkerStatusReport) (*WorkerStatusAck, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportWorkerStatus not implemented")
}

func (UnimplementedGatewayServiceServer) mustEmbedUnimplementedGatewayServiceServer() {}

type UnsafeGatewayServiceServer interface {
	mustEmbedUnimplementedGatewayServiceServer()
}

func RegisterGatewayServiceServer(s grpc.ServiceRegistrar, srv GatewayServiceServer) {
	s.RegisterService(&GatewayService_ServiceDesc, srv)
}

func _GatewayService_OpenSession_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(GatewayServiceServer).OpenSession(&gatewayServiceOpenSessionServer{ServerStream: stream})
}

func _GatewayService_ReportWorkerStatus_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(WorkerStatusReport)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).ReportWorkerStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/sessionrpc.GatewayService/ReportWorkerStatus",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(GatewayServiceServer).ReportWorkerStatus(ctx, req.(*WorkerStatusReport))
	}
	return interceptor(ctx, in, info, handler)
}

type GatewayService_OpenSessionClient interface {
	Send(*ClientEvent) error
	Recv() (*GatewayAck, error)
	grpc.ClientStream
}

type gatewayServiceOpenSessionClient struct {
	grpc.ClientStream
}

func (x *gatewayServiceOpenSessionClient) Send(m *ClientEvent) error {
	return x.ClientStream.SendMsg(m)
}

func (x *gatewayServiceOpenSessionClient) Recv() (*GatewayAck, error) {
	m := new(GatewayAck)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

type GatewayService_OpenSessionServer interface {
	Send(*GatewayAck) error
	Recv() (*ClientEvent, error)
	grpc.ServerStream
}

type gatewayServiceOpenSessionServer struct {
	grpc.ServerStream
}

func (x *gatewayServiceOpenSessionServer) Send(m *GatewayAck) error {
	return x.ServerStream.SendMsg(m)
}

func (x *gatewayServiceOpenSessionServer) Recv() (*ClientEvent, error) {
	m := new(ClientEvent)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var GatewayService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "sessionrpc.GatewayService",
	HandlerType: (*GatewayServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReportWorkerStatus",
			Handler:    _GatewayService_ReportWorkerStatus_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "OpenSession",
			Handler:       _GatewayService_OpenSession_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "sessionrpc.json",
}
