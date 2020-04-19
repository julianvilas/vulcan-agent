package queue

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SQSQueueManager represents an AWS SQS manager
type SQSQueueManager struct {
	svc      *sqs.SQS
	queueURL string
	pollTime time.Duration
	done     chan bool
	capacity chan int
	pause    chan bool
}

type sqsMessage struct {
	id            string
	body          string
	receiptHandle string
	message       *sqs.Message
	mgr           *SQSQueueManager
}

type queueData struct {
	region string
	name   string
}

// NewSQSQueueManager creates a new queue manager that reads messages from AWS SQS.
// The capacity channel sends the current capacity of the scheduler so
// that the queue manager can read the appropriate amount of messages.
// The pause channel sends the current paused status of the scheduler
// so that the queue manager can pause and resume reading messages.
func NewSQSQueueManager(queueARN string, pollTime time.Duration, endpoint string, region string, capacity chan int, pause chan bool) (*SQSQueueManager, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("error creating SQS session: %v", err)
	}

	qd := parseQueueARN(queueARN)
	if region == "" {
		region = qd.region
	}

	svc := sqs.New(sess, aws.NewConfig().WithRegion(region))

	if endpoint != "" {
		svc = sqs.New(sess, aws.NewConfig().WithRegion(region).WithEndpoint(endpoint))
	}

	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(qd.name),
	}
	resp, err := svc.GetQueueUrl(params)
	if err != nil {
		return nil, fmt.Errorf("error retrieving SQS queue URL: %v", err)
	}

	qm := &SQSQueueManager{
		svc:      svc,
		queueURL: *resp.QueueUrl,
		pollTime: pollTime,
		done:     make(chan bool),
		capacity: capacity,
		pause:    pause,
	}

	return qm, nil
}

// Messages process messages from a SQSQueueManager
func (qm *SQSQueueManager) Messages() (<-chan Message, <-chan error) {
	msgs := make(chan Message)
	errs := make(chan error)
	go qm.handleMessages(msgs, errs)
	return msgs, errs
}

func (qm *SQSQueueManager) handleMessages(msgs chan Message, errs chan error) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(qm.queueURL),
		// TODO: Move those constants to be config vars.
		WaitTimeSeconds:   aws.Int64(5),
		VisibilityTimeout: aws.Int64(30),
	}

	paused := false
	capacity := 0

	for {
		select {
		case <-qm.done:
			close(msgs)
			close(errs)
			return
		case paused = <-qm.pause:
		case capacity = <-qm.capacity:
		case <-time.After(qm.pollTime):
			if !paused && capacity > 0 {
				if capacity > 10 {
					capacity = 10
				}
				params.MaxNumberOfMessages = aws.Int64(int64(capacity))
				resp, err := qm.svc.ReceiveMessage(params)
				if err != nil {
					errs <- fmt.Errorf("error receiving SQS message: %v", err)
					continue
				}

				for _, message := range resp.Messages {
					msg := &sqsMessage{
						id:            *message.MessageId,
						body:          *message.Body,
						receiptHandle: *message.ReceiptHandle,
						message:       message,
						mgr:           qm,
					}
					msgs <- msg
				}
			}
		}
	}
}

// Close closes SQSQueueManager
func (qm *SQSQueueManager) Close() {
	qm.done <- true
}

// ID returns SQS message ID
func (msg *sqsMessage) ID() string {
	return msg.id
}

// Body returns SQS message Body
func (msg *sqsMessage) Body() string {
	return msg.body
}

// Delete deletes SQS message
func (msg *sqsMessage) Delete() error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(msg.mgr.queueURL),
		ReceiptHandle: aws.String(msg.receiptHandle),
	}

	_, err := msg.mgr.svc.DeleteMessage(params)
	if err != nil {
		return fmt.Errorf("error deleting SQS message with ID %q: %v", msg.id, err)
	}

	return nil
}

func parseQueueARN(queueARN string) queueData {
	arn := strings.Split(queueARN, ":")
	region := arn[3]
	name := arn[5]
	return queueData{
		name:   name,
		region: region,
	}
}
