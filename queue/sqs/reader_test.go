package sqs

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/adevinta/vulcan-agent/log"
	"github.com/adevinta/vulcan-agent/queue"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type sqsMock struct {
	sqsiface.SQSAPI
	MessageVisibilityChanger func(input *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error)
	MessageReceiver          func(ctx context.Context, input *sqs.ReceiveMessageInput, options ...request.Option) (*sqs.ReceiveMessageOutput, error)
	MessageDeleter           func(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
}

func (sq *sqsMock) ChangeMessageVisibility(input *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	return sq.MessageVisibilityChanger(input)
}

func (sq *sqsMock) ReceiveMessageWithContext(ctx context.Context, input *sqs.ReceiveMessageInput, options ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	return sq.MessageReceiver(ctx, input, options...)
}

func (sq *sqsMock) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return sq.MessageDeleter(input)
}

type messageProcessorMock struct {
	freeTokens     func() chan interface{}
	processMessage func(msg string, token interface{}) <-chan bool
}

func (mp *messageProcessorMock) FreeTokens() chan interface{} {
	return mp.freeTokens()
}

func (mp *messageProcessorMock) ProcessMessage(msg string, token interface{}) <-chan bool {
	return mp.processMessage(msg, token)
}

func TestReader_StartReading(t *testing.T) {
	type fields struct {
		RWMutex               *sync.RWMutex
		sqs                   sqsiface.SQSAPI
		visibilityTimeout     int
		processMessageQuantum int
		poolingInterval       int
		receiveParams         sqs.ReceiveMessageInput
		wg                    *sync.WaitGroup
		lastMessageReceived   *time.Time
		log                   log.Logger
		Processor             queue.MessageProcessor
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   chan error
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{
				RWMutex:               tt.fields.RWMutex,
				sqs:                   tt.fields.sqs,
				visibilityTimeout:     tt.fields.visibilityTimeout,
				processMessageQuantum: tt.fields.processMessageQuantum,
				poolingInterval:       tt.fields.poolingInterval,
				receiveParams:         tt.fields.receiveParams,
				wg:                    tt.fields.wg,
				lastMessageReceived:   tt.fields.lastMessageReceived,
				log:                   tt.fields.log,
				Processor:             tt.fields.Processor,
			}
			if got := r.StartReading(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reader.StartReading() = %v, want %v", got, tt.want)
			}
		})
	}
}
