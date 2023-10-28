/*
Copyright 2023 Adevinta
*/

package jobrunner

import (
	"errors"
	"testing"
)

func TestDiscard(t *testing.T) {
	proc := NewDiscard()
	for i := 0; i < 2; i++ {
		token, err := getToken(proc)
		if err != nil {
			t.Fatalf("could not get token: %v", err)
		}
		delete := <-proc.ProcessMessage(Message{}, token)
		if !delete {
			t.Errorf("message is not marked for deletion")
		}
	}
}

func getToken(proc *Discard) (token Token, err error) {
	select {
	case token := <-proc.FreeTokens():
		return token, nil
	default:
		return token, errors.New("no tokens available")
	}
}
