package posts_client

import (
	"context"

	"github.com/koliader/tellmi-sdk/errors/grpc"
	"github.com/koliader/tellmi-sdk/middleware"
	"github.com/koliader/tellmi-sdk/model"
	"github.com/koliader/tellmi-sdk/proto/pb"
	"google.golang.org/grpc/codes"
)

func (c *Client) CreatePost(ctx context.Context, req model.CreatePostReq, headers model.AuthHeaders) (*pb.Post, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.CreatePost(authCtx, &pb.CreatePostReq{
		Title:       req.Title,
		Description: req.Description,
		CategoryId:  req.CategoryID,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) ListPosts(ctx context.Context) (*pb.ListPostsRes, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.ListPosts(ctx, &pb.Empty{})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) GetPostByID(ctx context.Context, req *model.IDReq) (*pb.PostRow, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.GetPostByID(ctx, &pb.GetByIDReq{Id: req.ID})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) EditPost(ctx context.Context, req model.EditPostReq, headers model.AuthHeaders) (*pb.Post, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.EditPost(authCtx, &pb.EditPostReq{
		Id:          req.ID,
		Title:       req.Title,
		Description: req.Description,
		CategoryId:  req.CategoryID,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) DeletePost(ctx context.Context, req *model.IDReq, headers model.AuthHeaders) (*pb.Success, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.DeletePost(authCtx, &pb.GetByIDReq{Id: req.ID})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

// Categories

func (c *Client) CreateCategory(ctx context.Context, req model.CreateCategoryReq, headers model.AuthHeaders) (*pb.Category, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.CreateCategory(authCtx, &pb.CreateCategoryReq{Name: req.Name})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) ListCategories(ctx context.Context) (*pb.ListCategoriesRes, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.ListCategories(ctx, &pb.Empty{})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) EditCategory(ctx context.Context, req model.EditCategoryReq, headers model.AuthHeaders) (*pb.Success, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.EditCategory(authCtx, &pb.EditCategoryReq{
		Id:   req.ID,
		Name: req.Name,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

// Comments

func (c *Client) CreateComment(ctx context.Context, req model.CreateCommentReq, headers model.AuthHeaders) (*pb.Comment, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.CreateComment(authCtx, &pb.CreateCommentReq{
		Comment: req.Comment,
		PostId:  req.PostID,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) ListCommentsByPost(ctx context.Context, req *model.IDReq) (*pb.ListCommentsRes, *codes.Code, error) {
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.ListCommentsByPost(ctx, &pb.GetByIDReq{Id: req.ID})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) EditComment(ctx context.Context, req model.EditCommentReq, headers model.AuthHeaders) (*pb.Comment, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.EditComment(authCtx, &pb.EditCommentReq{
		Id:      req.ID,
		Comment: req.Comment,
	})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}

func (c *Client) DeleteComment(ctx context.Context, req *model.IDReq, headers model.AuthHeaders) (*pb.Success, *codes.Code, error) {
	authCtx := middleware.CreateAuthMetadata(ctx, headers.Token)
	if err := c.Connect(&ctx); err != nil {
		return nil, nil, err
	}

	res, err := postsGrpcServiceClient.DeleteComment(authCtx, &pb.GetByIDReq{Id: req.ID})
	if err != nil {
		return nil, errgrpc.GetErrorCode(err), errgrpc.ErrorResponse(err)
	}

	return res, nil, nil
}
