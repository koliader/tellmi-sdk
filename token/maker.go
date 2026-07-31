package token

import (
	"time"

	"github.com/google/uuid"
)

type Maker interface {
	CreateToken(id uuid.UUID, role string, duration time.Duration) (string, error)
	VerifyToken(tokenString string) (*Payload, error)
}
