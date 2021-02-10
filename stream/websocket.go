package stream

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lestrrat-go/backoff"

	"github.com/adevinta/vulcan-agent/log"
)

// WSDialerWithRetries provides retries with backoff and jitter
// when initiating a connection to a websocket.
type WSDialerWithRetries struct {
	*websocket.Dialer
	p backoff.Policy
	l log.Logger
}

// NewWSDialerWithRetries creates a WSDialer with the given retries parameters.
func NewWSDialerWithRetries(dialer *websocket.Dialer, log log.Logger, retries int, interval time.Duration) *WSDialerWithRetries {
	p := backoff.NewExponential(
		backoff.WithInterval(interval),
		backoff.WithJitterFactor(0.05),
		backoff.WithMaxRetries(retries),
	)
	return &WSDialerWithRetries{dialer, p, log}
}

// Dial wraps the dial function of the websocket dialer adding retries
// functionality.
func (ws *WSDialerWithRetries) Dial(urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
	var (
		conn *websocket.Conn
		resp *http.Response
	)
	err := withBackoff(ws.p, ws.l, func() error {
		var err error
		conn, resp, err = websocket.DefaultDialer.Dial(urlStr, requestHeader)
		return err
	})
	return conn, resp, err
}

func withBackoff(p backoff.Policy, l log.Logger, exec func() error) error {
	var err error
	retry, cancel := p.Start(context.Background())
	defer cancel()
	n := 0
	for {
		err = exec()
		if err == nil {
			return nil
		}
		select {
		case <-retry.Done():
			l.Errorf("stream websocket backoff finished unable to perform operation, retry number %d", n)
			return err
		case <-retry.Next():
			n++
			if n > 1 {
				l.Errorf("stream websocket backoff fired, retrying, retry number number %d", n-1)
			}
		}
	}
}
