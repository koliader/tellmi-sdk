package errgrpc

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func GetErrorCode(err error) *codes.Code {
	grpcStatus, _ := status.FromError(err)
	grpcCode := grpcStatus.Code()
	return &grpcCode
}

func ErrorResponse(err error) error {
	return errors.New(status.Convert(err).Message())
}
