package jobrunner

type MockProcessMessage func(msg Message, token Token) <-chan bool

type MessageProcessorMock struct {
	tokens         chan Token
	Messages       []Message
	processMessage MockProcessMessage
}

// NewMessageProcessorMock returns a mock message processor that uses the given
// ProcessMessage function, has the given number of total tokens and leaves
// free the specified ones.
func NewMessageProcessorMock(tokens int, free int, m MockProcessMessage) *MessageProcessorMock {
	t := make(chan Token, tokens)
	t <- token{}
	return &MessageProcessorMock{
		tokens:         t,
		processMessage: m,
	}
}

func (mp *MessageProcessorMock) FreeTokens() chan Token {
	return mp.tokens
}

func (mp *MessageProcessorMock) ProcessMessage(m Message, token Token) <-chan bool {
	mp.Messages = append(mp.Messages, m)
	return mp.processMessage(m, token)
}
