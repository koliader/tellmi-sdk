package users_client

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/koliader/tellmi-sdk/proto/pb"
)

var usersGrpcServiceClient pb.UsersClient

type Client struct {
	serviceAddress string
}

func NewClient(serviceAddress string) *Client {
	return &Client{
		serviceAddress: serviceAddress,
	}
}

func (c *Client) Connect(ctx *context.Context) error {
	conn, err := grpc.DialContext(*ctx, c.serviceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin": {}}]}`),
		grpc.WithBlock(),
	)
	if err != nil {
		usersGrpcServiceClient = nil
		return fmt.Errorf("connection to users gRPC service failed: %v", err)
	}
	if usersGrpcServiceClient != nil {
		conn.Close()
		return nil
	}
	usersGrpcServiceClient = pb.NewUsersClient(conn)
	return nil
}
