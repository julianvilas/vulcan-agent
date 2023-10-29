/*
Copyright 2023 Adevinta
*/

package checks

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adevinta/vulcan-agent/v2/log"
	"github.com/adevinta/vulcan-agent/v2/queue"

	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	ErrInvalidMessage       = errors.New("invalid message")
	ErrUnprocessableMessage = errors.New("unprocessable check message")
	// ErrMaxTimeNoRead is returned by a queue reader when there were no
	// messages available in the queue for more than the configured amount of
	// time.
	ErrMaxTimeNoRead = errors.New("no messages available in the queue for more than the max time")
)

type Message struct {
	Body      string
	TimesRead int
	// Processed will be written when the Runner stops processing the message.
	// The value written will be true if the Runner successfully processed the
	// message and it can be deleted from the queue. If the value written is false
	// the Runner finished processing the message with errors so
	Processed chan<- bool
}

type Queue interface {
	ReadMessage(ctx context.Context) (*Message, error)
}

type CheckRunner interface {
	Run(msg string, timesRead int) error
}

type ConsumerCfg struct {
	MaxConcurrentChecks int
	MaxReadTime         *time.Duration
}

type Consumer struct {
	*sync.RWMutex
	cfg                 ConsumerCfg
	sqs                 Queue
	receiveParams       sqs.ReceiveMessageInput
	wg                  *sync.WaitGroup
	lastMessageReceived *time.Time
	log                 log.Logger
	Runner              CheckRunner
	nProcessingMessages uint32
}

// NewConsumer creates a new Reader with the given processor, queueARN and config.
func NewConsumer(log log.Logger, cfg ConsumerCfg, queue Queue, processor CheckRunner) (*Consumer, error) {
	return &Consumer{
		cfg:                 cfg,
		RWMutex:             &sync.RWMutex{},
		Runner:              processor,
		log:                 log,
		wg:                  &sync.WaitGroup{},
		sqs:                 queue,
		lastMessageReceived: nil,
		nProcessingMessages: 0,
	}, nil
}

// Start starts reading messages from the queue. It will stop reading from the
// queue when the passed in context is canceled. The caller can use the returned
// channel to track when the reader stopped reading from the queue and all the
// checks being run are finished.
func (r *Consumer) Start(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	r.wg.Add(1)
	go r.consume(ctx, done)
	finished := make(chan error, 1)
	go func() {
		err := <-done
		r.wg.Wait()
		finished <- err
		close(finished)
	}()
	return finished
}

func (r *Consumer) consume(ctx context.Context, done chan<- error) {
	defer r.wg.Done()
	if r.Runner == nil {
		done <- errors.New("message processor is missing")
		close(done)
		return
	}
	tokens := make(chan struct{}, r.cfg.MaxConcurrentChecks)
	for i := 0; i < r.cfg.MaxConcurrentChecks; i++ {
		tokens <- struct{}{}
	}
	var (
		err error
		msg *Message
	)
loop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		case <-tokens:
			msg, err = r.readMessage(ctx)
			if err == ErrMaxTimeNoRead {
				r.log.Infof("reader stopped: max time without reading messages elapsed")
				break loop
			}
			if err != nil {
				break loop
			}
			r.wg.Add(1)
			go r.process(msg, tokens)
		}
	}
	done <- err
	close(done)
}

func (r *Consumer) process(msg *Message, done chan<- struct{}) {
	atomic.AddUint32(&r.nProcessingMessages, 1)
	defer func() {
		// Decrement the number of messages being processed, see:
		// https://golang.org/src/sync/atomic/doc.go?s=3841:3896#L87
		atomic.AddUint32(&r.nProcessingMessages, ^uint32(0))
		// Signal the message has been processed.
		done <- struct{}{}
		// Signal de goroutine has exited.
		r.wg.Done()
	}()
	if msg == nil {
		r.log.Errorf("cannot process nil message")
		return
	}
	err := r.Runner.Run(msg.Body, msg.TimesRead)
	del := true
	if err != nil {
		del = true
		r.log.Errorf("error processing message: %+v", err)
		if errors.Is(err, ErrUnprocessableMessage) {
			del = false
		}
	}
	msg.Processed <- del
}

func (r *Consumer) readMessage(ctx context.Context) (*Message, error) {
	maxTimeNoRead := r.cfg.MaxReadTime
	waitTime := int64(0)
	start := time.Now()
	var msg *Message
	for {
		r.receiveParams.WaitTimeSeconds = &waitTime
		readMsg, err := r.sqs.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, ErrInvalidMessage) {
				r.log.Errorf("error reading message: %w")
			}
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			return nil, err
		}
		if readMsg != nil {
			msg = readMsg
			break
		}
		// Check if we need to stop the reader because we exceed the maximun time
		// trying to read a message.
		now := time.Now()
		n := atomic.LoadUint32(&r.nProcessingMessages)
		if maxTimeNoRead != nil && now.Sub(start) > *maxTimeNoRead && n == 0 {
			return nil, queue.ErrMaxTimeNoRead
		}
	}
	now := time.Now()
	r.setLastMessageReceived(&now)
	return msg, nil
}

func (r *Consumer) setLastMessageReceived(t *time.Time) {
	r.Lock()
	r.lastMessageReceived = t
	r.Unlock()
}

// LastMessageReceived returns the time where the last message was received by
// the Reader. If no message was received so far it returns nil.
func (r *Consumer) LastMessageReceived() *time.Time {
	r.RLock()
	defer r.RUnlock()
	return r.lastMessageReceived
}

// SetMessageProcessor sets the queue's message processor. It must be
// set before calling [*Reader.StartReading].
func (r *Consumer) SetMessageProcessor(p CheckRunner) {
	r.Runner = p
}
