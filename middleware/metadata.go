package middleware

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

func CreateAuthMetadata(ctx context.Context, token string) context.Context {
	md := metadata.New(map[string]string{
		"authorization": fmt.Sprintf("Bearer %s", token),
	})
	return metadata.NewOutgoingContext(ctx, md)
}
