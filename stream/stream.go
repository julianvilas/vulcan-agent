package stream

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	agent "github.com/adevinta/vulcan-agent"
	"github.com/adevinta/vulcan-agent/log"
	metrics "github.com/adevinta/vulcan-metrics-client"
	// metrics "github.com/adevinta/vulcan-metrics-client"
)

// BufLen holds the length of the events buffer.
const BufLen = 10

// Stream represents a stream
type Stream struct {
	metricsClient metrics.Client
	endpoint      string
	stream        *websocket.Conn
	streamDialer  *WSDialerWithRetries
	timeout       time.Duration
	events        chan Message
	log           log.Logger
}

// Message represents a Stream message
type Message struct {
	Action  string `json:"action"`
	CheckID string `json:"check_id"`
}

// New initializes and connects a new Stream
func New(metricsClient metrics.Client, endpoint string,
	timeout time.Duration, l log.Logger, retries int, retryInterval time.Duration) (Stream, error) {

	dialer := NewWSDialerWithRetries(websocket.DefaultDialer, l, retries, retryInterval)
	conn, _, err := dialer.Dial(endpoint, http.Header{})
	if err != nil {
		return Stream{}, err
	}
	events := make(chan Message, BufLen)
	s := Stream{
		endpoint:     endpoint,
		stream:       conn,
		streamDialer: dialer,
		events:       events,
		timeout:      timeout,
		log:          l,
	}

	return s, nil
}

func (s *Stream) reconnect() error {
	s.log.Infof("stream reconnecting")
	conn, _, err := s.streamDialer.Dial(s.endpoint, http.Header{})
	if err != nil {
		return err
	}
	s.stream = conn
	done := s.connect(func(msg Message) bool {
		// We consider the agent to be reconnected if we receive any correct
		// message before the timeout passes.
		return true
	})
	err = <-done
	if err != nil {
		return err
	}
	s.log.Infof("stream connected")
	return nil
}

// HandleMessages handle Stream messages
func (s *Stream) HandleMessages(actions map[string]func(agent.Agent, string)) error {
	s.setStatus(StatusConnected)
	done := make(chan error)
	go func() {
		var err error
		ticker := time.NewTicker(100 * time.Millisecond)
	LOOP:
		for {
			select {
			case <-ticker.C:
				msg := Message{}
				// We set the timeout here in order to ensure we only read from
				// the websocket, at most, until the timeout elapses each time
				// we read.
				s.stream.SetReadDeadline(time.Now().Add(s.timeout))
				//rcvErr := websocket.ReadJSON(s.stream, &msg)
				rcvErr := s.stream.ReadJSON(&msg)
				if rcvErr != nil {
					if strings.HasPrefix(rcvErr.Error(), "json:") {
						s.log.WithError(rcvErr).WithFields(logrus.Fields{
							"message": msg,
						}).Warn("error decoding stream message")
						continue
					}
					s.log.WithError(rcvErr).WithFields(logrus.Fields{
						"message": msg,
					}).Warn("error receiving stream message")
					// We had a timeout error try to reconnect one time.
					s.log.Warn("stream connection lost")
					err = s.reconnect()
					if err != nil {
						break LOOP
					}
					continue
				}
				s.log.WithFields(logrus.Fields{
					"message": msg,
				}).Debug("stream message received")
				s.incrReceivedMssgs(msg, s.agent.ID())
				s.processMessage(actions, msg)
			case <-s.ctx.Done():
				s.log.Debug("agent context in is done in stream")
				break LOOP
			}
		}
		done <- err
	}()
	err := <-done
	if err != nil {
		s.log.WithError(err).Error("stream disconnected")
		s.setStatus(StatusDisconnected)
		// Disconnect agent.
		s.cancel()
		return err
	}
	return nil
}

// disconnect closes the connection to a Stream
// This method exists for testing purposes only.
func (s *Stream) disconnect() error {
	return s.stream.Close()
}

// Status returns the status of a Stream
func (s *Stream) Status() string {
	mu.Lock()
	defer mu.Unlock()
	return s.status
}

// setStatus set the status of a Stream
func (s *Stream) setStatus(status string) {
	mu.Lock()
	defer mu.Unlock()
	s.status = status
}

// incrReceivedMssgs increments the metric for
// received messages from stream broadcasting.
func (s *Stream) incrReceivedMssgs(msg Message, agentID string) {
	s.metricsClient.Push(metrics.Metric{
		Name:  "vulcan.stream.mssgs.received",
		Typ:   metrics.Count,
		Value: 1,
		Tags: []string{
			"component:agent",
			fmt.Sprint("action:", msg.Action),
			fmt.Sprint("agentid:", agentID),
		},
	})
}
