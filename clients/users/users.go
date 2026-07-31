package users_client

import (
	"context"

	"github.com/koliader/tellmi-sdk/errors/grpc"
	"github.com/koliader/tellmi-sdk/middleware"
	"github.com/koliader/tellmi-sdk/model"
	"github.com/koliader/tellmi-sdk/proto/pb"
	"google.golang.org/grpc/codes"
)

func (c *Client) Register(ctx context.Context, req model.RegisterReq) (*pb.AuthRes, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := usersGrpcServiceClient.Register(ctx, &pb.RegisterReq{
		Username: req.Username,
		Password: req.Password,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) Login(ctx context.Context, req model.LoginReq) (*pb.AuthRes, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := usersGrpcServiceClient.Login(ctx, &pb.LoginReq{
		Username: req.Username,
		Password: req.Password,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) Refresh(ctx context.Context, req model.RefreshReq) (*pb.RefreshRes, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := usersGrpcServiceClient.Refresh(ctx, &pb.RefreshReq{
		RefreshToken: req.RefreshToken,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) GetUserById(ctx context.Context, req *model.IDReq, headers model.AuthHeaders) (*pb.UserRes, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := usersGrpcServiceClient.GetUserById(authCtx, &pb.IdReq{Id: req.ID})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) ListUsers(ctx context.Context, headers model.AuthHeaders) (*pb.ListUserRes, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := usersGrpcServiceClient.ListUsers(authCtx, &pb.Empty{})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) UpdateUser(ctx context.Context, req model.UpdateUserReq, headers model.AuthHeaders) (*pb.UserRes, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := usersGrpcServiceClient.UpdateUser(authCtx, &pb.UpdateUserReq{
		Username: req.Username,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}
