package jobrunner

import "sync"

// MessageProcessorRecorder implements a message processor that records the
// the messages it processes.
type MessageProcessorRecorder struct {
	tokens chan Token
	mu     sync.RWMutex
	msgs   []Message
	Fails  int
}

func NewMessageProcessorRecorder() *MessageProcessorRecorder {
	tokens := make(chan Token, 1)
	tokens <- token{}
	return &MessageProcessorRecorder{
		tokens: tokens,
	}
}

func (rec *MessageProcessorRecorder) FreeTokens() chan Token {
	return rec.tokens
}

func (rec *MessageProcessorRecorder) ProcessMessage(msg Message, token Token) <-chan bool {
	c := make(chan bool)
	go func() {
		ok := rec.success()
		if ok {
			rec.recordMessage(msg)
		}
		rec.tokens <- token
		c <- ok
	}()
	return c
}

func (rec *MessageProcessorRecorder) success() bool {
	rec.mu.Lock()
	defer rec.mu.Unlock()

	if rec.Fails == 0 {
		return true
	}
	rec.Fails--
	return false
}

func (rec *MessageProcessorRecorder) recordMessage(msg Message) {
	rec.mu.Lock()
	defer rec.mu.Unlock()

	rec.msgs = append(rec.msgs, msg)
}

func (rec *MessageProcessorRecorder) Messages() []Message {
	rec.mu.RLock()
	defer rec.mu.RUnlock()

	return rec.msgs
}
