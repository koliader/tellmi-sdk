package token

import (
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
)

type Payload struct {
	ID        uuid.UUID `json:"id"`
	Role      string    `json:"role"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiredAt time.Time `json:"expired_at"`
}

func NewPayload(id uuid.UUID, role string, duration time.Duration) (*Payload, error) {
	payload := &Payload{
		ID:        id,
		Role:      role,
		IssuedAt:  time.Now(),
		ExpiredAt: time.Now().Add(duration),
	}
	return payload, nil
}

func (p *Payload) Valid() error {
	if time.Now().After(p.ExpiredAt) {
		return jwt.NewValidationError("token is expired", jwt.ValidationErrorExpired)
	}
	return nil
}
