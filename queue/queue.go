/*
Copyright 2019 Adevinta
*/

package queue

import (
	"context"
	"errors"
	"time"

	"github.com/adevinta/vulcan-agent/v2/jobrunner"
)

// ErrMaxTimeNoRead is returned by a queue reader when there were no
// messages available in the queue for more than the configured amount of
// time.
var ErrMaxTimeNoRead = errors.New("no messages available in the queue for more than the max time")

// MessageProcessor defines the methods needed by a queue reader
// implementation to process the messages it reads.
//
// FreeTokens returns a channel that can be used to get a token to
// call ProcessMessage.
//
// ProcessMessage processes the message given a token that must be
// obtained from FreeTokens. ProcessMessage is in charge of returning
// the token once the message has been processed. When the message has
// been processed the returned channel will indicate if the message
// must be deleted from the queue or not.
type MessageProcessor interface {
	// FreeTokens returns a channel that can be read to get a token that
	// signals that there is a free slot for the processor to process a new
	// message.
	FreeTokens() chan jobrunner.Token
	ProcessMessage(msg jobrunner.Message, token jobrunner.Token) <-chan bool
}

// Reader defines the methods that all the queue reader
// implementations must fulfill.
//
// StartReading starts reading messages from the queue. The caller can
// use the returned channel to track when the reader stops reading
// from the queue and all the messages have been processed.
//
// LastMessageReceived returns the time when the last message was
// read. If no messages have been read it returns nil.
type Reader interface {
	StartReading(ctx context.Context) <-chan error
	LastMessageReceived() *time.Time
}

// Writer defines the functions that a queue writer must implement.
type Writer interface {
	Write(body string) error
}
