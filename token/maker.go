package token

import "time"

type Maker interface {
	CreateToken(username string, role string, duration time.Duration) (string, error)
	VerifyToken(tokenString string) (*Payload, error)
}
