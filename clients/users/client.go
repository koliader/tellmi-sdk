package users_client

import (
	"context"
	"fmt"

	"github.com/koliader/tellmi-sdk/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
