/*
Copyright 2023 Adevinta
*/

package chanqueue

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/adevinta/vulcan-agent/v2/jobrunner"
	"github.com/adevinta/vulcan-agent/v2/queue"
)

func TestChanQueue(t *testing.T) {
	msgs := []jobrunner.Message{
		{
			Body:      "foo",
			TimesRead: 1,
		},
		{
			Body:      "bar",
			TimesRead: 1,
		},
		{
			Body:      "foobar",
			TimesRead: 1,
		},
	}

	rec := jobrunner.NewMessageProcessorRecorder()
	q := New(rec)
	q.MaxTimeNoRead = 100 * time.Millisecond

	for _, msg := range msgs {
		q.Write(msg.Body)
	}

	err := <-q.StartReading(context.Background())

	if err != queue.ErrMaxTimeNoRead {
		t.Errorf("unexpected error: want: queue.ErrMaxTimeNoRead, got: %v", err)
	}

	if diff := cmp.Diff(msgs, rec.Messages(), cmpopts.SortSlices(messageLess)); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}
}

func TestChanQueue_multiple_reads(t *testing.T) {
	msgs := []jobrunner.Message{
		{
			Body:      "foo",
			TimesRead: 1,
		},
		{
			Body:      "bar",
			TimesRead: 1,
		},
		{
			Body:      "foobar",
			TimesRead: 1,
		},
	}
	const firstBatch = 1

	rec := jobrunner.NewMessageProcessorRecorder()
	q := New(rec)
	q.MaxTimeNoRead = 100 * time.Millisecond

	// First batch.
	for _, msg := range msgs[:firstBatch] {
		q.Write(msg.Body)
	}

	err := <-q.StartReading(context.Background())
	if err != queue.ErrMaxTimeNoRead {
		t.Errorf("unexpected error: want: queue.ErrMaxTimeNoRead, got: %v", err)
	}

	if diff := cmp.Diff(msgs[:firstBatch], rec.Messages(), cmpopts.SortSlices(messageLess)); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}

	// Second batch.
	for _, msg := range msgs[firstBatch:] {
		q.Write(msg.Body)
	}

	err = <-q.StartReading(context.Background())
	if err != queue.ErrMaxTimeNoRead {
		t.Errorf("unexpected error: want: queue.ErrMaxTimeNoRead, got: %v", err)
	}

	if diff := cmp.Diff(msgs, rec.Messages(), cmpopts.SortSlices(messageLess)); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}
}

func TestChanQueue_context(t *testing.T) {
	rec := jobrunner.NewMessageProcessorRecorder()
	q := New(rec)
	q.MaxTimeNoRead = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := <-q.StartReading(ctx)

	if err != context.DeadlineExceeded {
		t.Errorf("unexpected error: want: context.DeadlineExceeded, got: %v", err)
	}

	if got := rec.Messages(); got != nil {
		t.Errorf("unexpected msgs: %#v", got)
	}
}

func TestChanQueue_retry(t *testing.T) {
	msg := jobrunner.Message{
		Body:      "foo",
		TimesRead: 5,
	}

	rec := jobrunner.NewMessageProcessorRecorder()
	rec.Fails = 4

	q := New(rec)
	q.MaxTimeNoRead = 100 * time.Millisecond

	q.Write(msg.Body)

	err := <-q.StartReading(context.Background())

	if err != queue.ErrMaxTimeNoRead {
		t.Errorf("unexpected error: want: queue.ErrMaxTimeNoRead, got: %v", err)
	}

	if diff := cmp.Diff([]jobrunner.Message{msg}, rec.Messages(), cmpopts.SortSlices(messageLess)); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%v", diff)
	}
}

func messageLess(a, b jobrunner.Message) bool {
	return a.Body < b.Body
}
