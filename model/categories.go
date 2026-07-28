package model

type CreateCategoryReq struct {
	Name string `json:"name" binding:"required"`
}

type EditCategoryReq struct {
	ID   int64  `json:"id"   binding:"required"`
	Name string `json:"name" binding:"required"`
}
