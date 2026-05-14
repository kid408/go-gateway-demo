package sessionrpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DefaultDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(JSONCodec())),
	}
}

func DefaultServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ForceServerCodec(JSONCodec()),
	}
}

func DialContext(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	base := append([]grpc.DialOption{}, DefaultDialOptions()...)
	base = append(base, grpc.WithBlock())
	base = append(base, opts...)

	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return grpc.DialContext(dctx, target, base...)
}
