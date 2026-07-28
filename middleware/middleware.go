package middleware

import "github.com/koliader/tellmi-sdk/token"

type GrpcMiddleware struct {
	tokenMaker token.Maker
}

func NewMiddleware(tokenMaker token.Maker) *GrpcMiddleware {
	return &GrpcMiddleware{
		tokenMaker: tokenMaker,
	}
}
