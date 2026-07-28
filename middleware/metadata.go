package middleware

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func CreateAuthMetadata(ctx context.Context, token string) context.Context {
	md := metadata.New(map[string]string{
		"authorization": token,
	})
	return metadata.NewOutgoingContext(ctx, md)
}
