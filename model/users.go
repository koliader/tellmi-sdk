package model

type RegisterReq struct {
	Username string `json:"username" binding:"required,min=3"`
	Password string `json:"password" binding:"required"`
}

type LoginReq struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type UpdateUserReq struct {
	ID       string `json:"id"       binding:"required"`
	Username string `json:"username" binding:"required,min=3"`
}
