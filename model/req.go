package model

type IDReq struct {
	ID string `uri:"id" binding:"required"`
}

type AuthHeaders struct {
	Token string
}

type PaginationReq struct {
	Limit  int64 `uri:"limit" form:"limit"`
	Offset int64 `uri:"offset" form:"offset"`
}
