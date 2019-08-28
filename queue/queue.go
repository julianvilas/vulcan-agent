package queue

// Message represents a queue message
type Message interface {
	ID() string
	Body() string
	Delete() error
}

// Manager represents a queue message manager
type Manager interface {
	Messages() (<-chan Message, <-chan error)
	Close()
}
