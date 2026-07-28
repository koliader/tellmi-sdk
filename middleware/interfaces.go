package middleware

import (
	"context"

	"github.com/koliader/tellmi-sdk/token"
)

type GRPCMiddleware interface {
	AuthorizeUser(ctx context.Context) (*token.Payload, error)
	AuthorizeAdmin(ctx context.Context) (*token.Payload, error)
}
