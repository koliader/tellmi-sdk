package errsvc

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ErrorResponse(code codes.Code, msg string, a ...any) error {
	if len(a) > 0 {
		return status.Errorf(code, "%s: %s", msg, fmt.Sprint(a...))
	}
	return status.Errorf(code, "%s", msg)
}

func AuthError(err error) error {
	return status.Errorf(codes.Unauthenticated, "authentication error: %v", err)
}
