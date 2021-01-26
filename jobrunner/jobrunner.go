package jobrunner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/adevinta/vulcan-agent/stateupdater"
)

var (
	// ErrInvalidToken is returned when caller of the ProcessMessage function
	// does not pass a valid token written by Runner in its Token channel.
	ErrInvalidToken = errors.New("invalid token")

	// ErrCheckWithSameID is returned when the runner is about to
	ErrCheckWithSameID = errors.New("check with a same ID is already runing")
)

type token = struct{}

type checkAborter struct {
	cancels sync.Map
}

func (c *checkAborter) Add(checkID string, cancel context.CancelFunc) error {
	_, exists := c.cancels.LoadOrStore(checkID, cancel)
	if exists {
		return ErrCheckWithSameID
	}
	return nil
}

func (c *checkAborter) Remove(checkID string) {
	c.cancels.Delete(checkID)
}

func (c *checkAborter) Exist(checkID string) bool {
	_, ok := c.cancels.Load(checkID)
	return ok
}

func (c *checkAborter) Abort(checkID string) {
	v, ok := c.cancels.Load(checkID)
	if !ok {
		return
	}
	cancel := v.(context.CancelFunc)
	cancel()
}

// BackendRunResult defines the info that must be returned when a check is
// finished.
type BackendRunResult struct {
	Output []byte
	Error  error
}

// Backend defines the shape of the backend needed by the CheckRunner to execute
// a Job.
type Backend interface {
	Run(ctx context.Context, job JobParams) (<-chan BackendRunResult, error)
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// ChecksLogsStore provides functionality to store the logs of a check.
type ChecksLogsStore interface {
	UpdateCheckRaw(checkID, scanID string, scanStartTime time.Time, raw []byte) (string, error)
}

type CheckStateUpdater interface {
	UpdateState(stateupdater.CheckState) error
}

type Runner struct {
	Backend Backend
	// Tokens contains the currently free tokens of runner to run a check. Any
	// caller of the Run function must take a token from this channel before
	// actually calling "Run" in order to ensure there are no more than
	// maxTokens jobs running at the same time.
	Tokens       chan interface{}
	Log          Logger
	CheckUpdater CheckStateUpdater
	LogStore     ChecksLogsStore
	cAborter     checkAborter
	// wg is used to allow a caller to wait for all jobs that are running to be
	// finish.
	wg  *sync.WaitGroup
	ctx context.Context
}

// NewRunner creates a Runner initialized with the given log, backend and
// maximun number of tokens. The maximum number of tokens is the maximun number
// jobs that the Runner can execute at the same time.
func NewRunner(ctx context.Context, log Logger, backend Backend, checkUpdater CheckStateUpdater,
	logsStore ChecksLogsStore, maxTokens int) *Runner {
	var tokens = make(chan interface{}, maxTokens)
	for i := 0; i < maxTokens; i++ {
		tokens <- token{}
	}
	return &Runner{
		Backend:      backend,
		Tokens:       tokens,
		CheckUpdater: checkUpdater,
		LogStore:     logsStore,
		cAborter: checkAborter{
			cancels: sync.Map{},
		},
		Log: log,
		wg:  &sync.WaitGroup{},
		ctx: ctx,
	}
}

// ProcessMessage executes the job specified in a message given a free token
// that must be obtained from the Tokens channel. The func does not actually do
// anything with the token, the parameter is present just to make obvious that
// there must be free tokens on the channel before calling this method. When the
// message if processed the channel returned will indicate if the message must
// be deleted or not.
func (cr *Runner) ProcessMessage(msg string, token interface{}) <-chan bool {
	cr.wg.Add(1)
	var processed = make(chan bool, 1)
	go cr.runJob(msg, token, processed)
	return processed
}

func (cr *Runner) runJob(msg string, t interface{}, processed chan bool) {
	// Check the token is valid.
	if _, ok := t.(token); !ok {
		cr.finishJob("", processed, false, ErrInvalidToken)
	}
	j := &JobParams{}
	err := j.UnmarshalJSON([]byte(msg))
	if err != nil {
		err = fmt.Errorf("error unmarshaling message %+v", msg)
		cr.finishJob("", processed, true, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	err = cr.cAborter.Add(j.CheckID, cancel)
	// The above function can only return an error if the check already
	// exists.
	if err != nil {
		cr.finishJob(j.CheckID, processed, true, err)
		return
	}

	startTime := time.Now()
	finished, err := cr.Backend.Run(ctx, *j)
	if err != nil {
		cr.cAborter.Remove(j.CheckID)
		cr.finishJob(j.CheckID, processed, false, err)
		return
	}
	var logsLink string
LOOP:
	for {
		select {
		case res := <-finished:
			logsLink, err = cr.LogStore.UpdateCheckRaw(j.CheckID, j.ScanID, startTime, res.Output)
			if err != nil {
				err = fmt.Errorf("error storing log for check %+v", err)
				break LOOP
			}
			// We don't set any kind of status here we just comunicate
			// where are the logs of the check.
			err = cr.CheckUpdater.UpdateState(stateupdater.CheckState{
				ID:  j.CheckID,
				Raw: &logsLink,
			})
			break LOOP
		}
	}
	delete := true
	if err != nil {
		delete = false
	}
	cr.cAborter.Remove(j.CheckID)
	cr.finishJob(j.CheckID, processed, delete, err)
}

func (cr *Runner) finishJob(checkID string, processed chan<- bool, delete bool, err error) {
	defer cr.wg.Done()
	if err != nil && checkID != "" {
		cr.Log.Errorf("error %+v running check_id %s", err, checkID)
	}
	if err != nil && checkID == "" {
		cr.Log.Errorf("error %+v running job invalid message")
	}
	// This write must not block ever.
	select {
	case cr.Tokens <- 1:
	default:
		cr.Log.Errorf("error, unexpected lock when writting to the tokens channel")
	}
	processed <- delete
}
