package rabbitmq

import "github.com/google/uuid"

type UserUpdated struct {
	ID          uuid.UUID `json:"id"`
	NewUsername string    `json:"newUsername"`
}

type UserCreated struct {
	ID       uuid.UUID `json:"id"`
	Username string    `json:"username"`
}

type MessageSender interface {
	SendMessage(queueName string, message []byte) error
}
