package model

type CreateCommentReq struct {
	Comment string `json:"comment" binding:"required"`
	PostID  int64  `json:"postId"  binding:"required"`
}

type EditCommentReq struct {
	ID      int64  `json:"id"      binding:"required"`
	Comment string `json:"comment" binding:"required"`
}
