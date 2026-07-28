package model

type CreatePostReq struct {
	Title       string `json:"title"       binding:"required"`
	Description string `json:"description" binding:"required"`
	CategoryID  int64  `json:"categoryId"  binding:"required"`
}

type EditPostReq struct {
	ID          int64  `json:"id"          binding:"required"`
	Title       string `json:"title"       binding:"required"`
	Description string `json:"description" binding:"required"`
	CategoryID  int64  `json:"categoryId"  binding:"required"`
}
