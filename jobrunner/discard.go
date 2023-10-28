package jobrunner

// Discard is a Job runner that discards all the received
// messages.
type Discard struct {
	tokens chan Token
}

// NewDiscard returns a Job runner that discards all the received
// messages.
func NewDiscard() *Discard {
	tokens := make(chan Token, 1)
	tokens <- token{}
	return &Discard{
		tokens: tokens,
	}
}

// FreeTokens returns a channel that can be used to get a free token
// to call ProcessMessage.
func (proc *Discard) FreeTokens() chan Token {
	return proc.tokens
}

// ProcessMessage discards the message and returns the provided token.
func (proc *Discard) ProcessMessage(_ Message, token Token) <-chan bool {
	c := make(chan bool)
	go func() {
		select {
		case proc.tokens <- token:
		default:
			panic("could not return token")
		}
		c <- true
	}()
	return c
}
