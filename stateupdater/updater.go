/*
Copyright 2021 Adevinta
*/

package stateupdater

import (
	"encoding/json"
	"sync"
)

const (
	StatusCreated      = "CREATED"
	StatusQueued       = "QUEUED"
	StatusAssigned     = "ASSIGNED"
	StatusRunning      = "RUNNING"
	StatusTimeout      = "TIMEOUT"
	StatusAborted      = "ABORTED"
	StatusPurging      = "PURGING"
	StatusKilled       = "KILLED"
	StatusFailed       = "FAILED"
	StatusFinished     = "FINISHED"
	StatusMalformed    = "MALFORMED"
	StatusInconclusive = "INCONCLUSIVE"
)

var terminalStatuses = map[string]struct{}{
	"MALFORMED":    {},
	"ABORTED":      {},
	"KILLED":       {},
	"FAILED":       {},
	"FINISHED":     {},
	"TIMEOUT":      {},
	"INCONCLUSIVE": {},
}

// IsTerminalStatus return if a given check status, like RUNNING is terminal or
// not.
func IsTerminalStatus(checkStatus string) bool {
	_, is := terminalStatuses[checkStatus]
	return is
}

type terminalStatusChecks struct {
	sync.RWMutex
	checksTerminated map[string]struct{}
}

func (t *terminalStatusChecks) addCheckTerminated(ID string) {
	t.Lock()
	defer t.Unlock()
	t.checksTerminated[ID] = struct{}{}
}

// IsCheckTerminated returns true if the given check was registered to be in a
// terminal status.
func (t *terminalStatusChecks) IsCheckTerminated(ID string) bool {
	t.RLock()
	_, ok := t.checksTerminated[ID]
	t.RUnlock()
	return ok
}

// RemoveChecks removes the check with the given ID from the list of terminated
// checks if present.
func (t *terminalStatusChecks) RemoveCheck(ID string) {
	t.Lock()
	delete(t.checksTerminated, ID)
	t.Unlock()
	return
}

// CheckState defines all the possible fields of a check state sent to the
// status queue.
type CheckState struct {
	ID       string   `json:"id" validate:"required"`
	Status   *string  `json:"status,omitempty"`
	AgentID  *string  `json:"agent_id,omitempty"`
	Report   *string  `json:"report,omitempty"`
	Raw      *string  `json:"raw,omitempty"`
	Progress *float32 `json:"progress,omitempty"`
}

// QueueWriter defines the queue services used by and
// updater to send the status updates.
type QueueWriter interface {
	Write(body string) error
}

// Updater takes a CheckState an send its to a queue using the defined queue
// writer.
type Updater struct {
	qw QueueWriter
	terminalStatusChecks
}

// New creates a new updater using the provided queue writer.
func New(qw QueueWriter) *Updater {
	return &Updater{qw, terminalStatusChecks{
		checksTerminated: map[string]struct{}{},
	}}
}

// UpdateState updates the state of tha check into the underlaying queue.
func (u *Updater) UpdateState(s CheckState) error {
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	err = u.qw.Write(string(body))
	if err != nil {
		return err
	}
	if s.ID != "" {
		u.addCheckTerminated(s.ID)
	}
	return nil
}
