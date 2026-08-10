package errdb

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	ForeignKeyViolation = "23503"
	UniqueViolation     = "23505"
	RedisDeleteKeyError = "error to delete redis key"
	RedisSetKeyError    = "error to set redis key"
)

var ErrRecordNotFound = pgx.ErrNoRows
var ErrCacheMiss = errors.New("cache miss")

var ErrUniqueViolation = &pgconn.PgError{
	Code: UniqueViolation,
}

func ErrorCode(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	return ""
}
