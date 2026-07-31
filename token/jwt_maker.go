package token

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
)

var ErrInvalidToken = errors.New("invalid token")

type JWTMaker struct {
	secretKey string
}

func NewJWTMaker(secretKey string) (Maker, error) {
	if len(secretKey) < 32 {
		return nil, fmt.Errorf("invalid key size: must be at least 32 characters")
	}
	return &JWTMaker{secretKey}, nil
}

func (m *JWTMaker) CreateToken(id uuid.UUID, role string, duration time.Duration) (string, error) {
	payload, err := NewPayload(id, role, duration)
	if err != nil {
		return "", err
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, payload)
	return token.SignedString([]byte(m.secretKey))
}

func (m *JWTMaker) VerifyToken(tokenString string) (*Payload, error) {
	token, err := jwt.ParseWithClaims(
		tokenString,
		&Payload{},
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, ErrInvalidToken
			}
			return []byte(m.secretKey), nil
		},
	)
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Payload)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}
