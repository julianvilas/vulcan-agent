package sqs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/adevinta/vulcan-agent/config"
	"github.com/adevinta/vulcan-agent/log"
	"github.com/adevinta/vulcan-agent/queue"
)

const (
	readMessageWaitTime      = 1  // in seconds.
	defaultVisibilityTimeout = 30 // in seconds
	// The processMessageQuantum must always be less than the
	// defaultVisibilityTimeout by, at least, a few seconds
	processMessageQuantum = 10 // in seconds
)

type Reader struct {
	sqs           sqsiface.SQSAPI
	receiveParams sqs.ReceiveMessageInput
	wg            *sync.WaitGroup
	log           log.Logger
	Processor     queue.MessageProcessor
	Config        config.SQSConfig
	ForceExit     chan struct{}
}

// New creates a new Reader with the given processor, queueARN and config.
func New(processor queue.MessageProcessor, queueARN string, config config.SQSConfig) (*Reader, error) {
	var consumer *Reader
	sess, err := session.NewSession()
	if err != nil {
		err = fmt.Errorf("error creating AWSSSession, %w", err)
		return consumer, err
	}

	arn, err := arn.Parse(queueARN)
	if err != nil {
		return nil, fmt.Errorf("error parsing SQS queue ARN: %v", err)
	}

	awsCfg := aws.NewConfig()
	if arn.Region != "" {
		awsCfg = awsCfg.WithRegion(arn.Region)
	}
	if config.Endpoint != "" {
		awsCfg = awsCfg.WithEndpoint(config.Endpoint)
	}

	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(arn.Resource),
	}
	if arn.AccountID != "" {
		params.SetQueueOwnerAWSAccountId(arn.AccountID)
	}

	srv := sqs.New(sess, awsCfg)
	resp, err := srv.GetQueueUrl(params)
	if err != nil {
		return consumer, fmt.Errorf("error retrieving SQS queue URL: %v", err)
	}

	receiveParams := sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(*resp.QueueUrl),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(readMessageWaitTime),
		VisibilityTimeout:   aws.Int64(defaultVisibilityTimeout),
	}
	return &Reader{
		Processor:     processor,
		Config:        config,
		ForceExit:     make(chan struct{}),
		wg:            &sync.WaitGroup{},
		receiveParams: receiveParams,
		sqs:           srv,
	}, nil

}

// StartReading starts reading messages from the sqs. It reads messages only
// when there are free tokens in the message processor. It will stop reading
// from the queue when the passed in context is canceled. The caller can use the
// returned channel to track when the reader stopped reading from the queue and
// all the messages it is tracking are finished processing.
func (r *Reader) StartReading(ctx context.Context) chan error {
	var done = make(chan error, 1)
	go r.read(ctx, done, r.ForceExit)
	var finished = make(chan error, 1)
	go func() {
		err := <-finished
		r.wg.Wait()
		finished <- err
	}()
	return finished
}

func (r *Reader) read(ctx context.Context, done chan<- error, stop <-chan struct{}) {
	var (
		err error
		msg *sqs.Message
	)
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		case token := <-r.Processor.FreeTokens():
			msg, err = r.readMessage(ctx)
			if err == context.Canceled {
				break loop
			}
			if err != nil {
				break loop
			}
			if msg == nil {
				continue loop
			}
			r.wg.Add(1)
			go r.processAndTrack(msg, token, stop)
		}
	}
	done <- err
	close(done)
}

func (r *Reader) readMessage(ctx context.Context) (*sqs.Message, error) {
	resp, err := r.sqs.ReceiveMessageWithContext(ctx, &r.receiveParams)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			return nil, context.Canceled
		}
		return nil, err
	}
	if len(resp.Messages) == 0 {
		r.receiveParams.WaitTimeSeconds = aws.Int64(readMessageWaitTime)
		return nil, nil
	}
	// We read only 1 message at time.
	return resp.Messages[0], nil
}

func (r *Reader) processAndTrack(msg *sqs.Message, token interface{}, stop <-chan struct{}) {
	defer r.wg.Done()
	if msg.Body == nil {
		r.log.Errorf("unexpected empty body message from sqs")
		// Invalid message delete from queue without processing.
		_, err := r.sqs.DeleteMessage(&sqs.DeleteMessageInput{
			ReceiptHandle: msg.ReceiptHandle,
			QueueUrl:      r.receiveParams.QueueUrl,
		})
		if err != nil {
			r.log.Errorf("ErrorDeletingProcessedMessage", err.Error())
		}
	}
	processed := r.Processor.ProcessMessage(*msg.Body, token)
	timer := time.NewTimer(processMessageQuantum)
loop:
	for {
		select {
		case <-stop:
			r.log.Infof("messager reader forced stop")
			break loop
		case <-timer.C:
			extime := int64(defaultVisibilityTimeout)
			input := &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          r.receiveParams.QueueUrl,
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: &extime,
			}
			output, err := r.sqs.ChangeMessageVisibility(input)
			if err != nil {
				r.log.Errorf("error extending message timeout", err)
				break loop
			}
			r.log.Debugf("message visibility extended: %s", output.String())
			timer.Reset(processMessageQuantum)
		case delete := <-processed:
			if !delete {
				break loop
			}
			input := &sqs.DeleteMessageInput{
				QueueUrl:      r.receiveParams.QueueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			}
			output, err := r.sqs.DeleteMessage(input)
			if err != nil {
				r.log.Errorf("error extending message timeout", err)
				break loop
			}
			r.log.Debugf("message deleted: %s", output.String())
			break loop
		}
	}
}
