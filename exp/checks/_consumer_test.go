/*
Copyright 2021 Adevinta
*/

package checks

import (
	"errors"
	"time"
)

var ErrMockError = errors.New("mockerror")

func strToPtr(input string) *string {
	return &input
}

func intToPtr(in int64) *int64 {
	return &in
}

func durationToPtr(t time.Duration) *time.Duration {
	return &t
}
