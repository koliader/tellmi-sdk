package rabbitmq

type UserUpdated struct {
	Username    string `json:"username"`
	NewUsername string `json:"newUsername"`
}

type UserCreated struct {
	Username string `json:"username"`
}

type MessageSender interface {
	SendMessage(queueName string, message []byte) error
}
