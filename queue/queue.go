package queue

import "context"

// MessageProcessor defines the methods needed by a queue reader implementation
// to process the messages it reads.
type MessageProcessor interface {
	FreeTokens() chan interface{}
	ProcessMessage(msg string, token interface{}) <-chan bool
}

// Reader defines the functions that all the concreate queue reader
// implementations must fullfil.
type Reader interface {
	StartReading(ctx context.Context) <-chan error
}
