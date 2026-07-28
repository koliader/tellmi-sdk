package model

type RefreshReq struct {
	RefreshToken string `json:"refreshToken" binding:"required"`
}
