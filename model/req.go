package model

type IDReq struct {
	ID int64 `uri:"id" binding:"required"`
}

type AuthHeaders struct {
	Token string
}
