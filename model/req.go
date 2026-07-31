package model

type IDReq struct {
	ID string `uri:"id" binding:"required"`
}

type AuthHeaders struct {
	Token string
}
