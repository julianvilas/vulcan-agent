package jobrunner

import (
	"context"
	"encoding/json"
	"time"
)

// JobParams stores the information necessary to create a new check job.
type JobParams struct {
	CheckID       string            `json:"check_id"`      // Required
	ScanID        string            `json:"scan_id"`       // Required
	ScanStartTime time.Time         `json:"start_time"`    // Required
	Image         string            `json:"image"`         // Required
	Target        string            `json:"target"`        // Required
	Timeout       int               `json:"timeout"`       // Required
	AssetType     string            `json:"assettype"`     // Optional
	Options       string            `json:"options"`       // Optional
	RequiredVars  []string          `json:"required_vars"` // Optional
	Metadata      map[string]string `json:"metadata"`      // Optional
}

// UnmarshalJSON handles the special format for scanStartTime and also replace
// the scan_id with the check_id if the former is not found.
func (jp *JobParams) UnmarshalJSON(data []byte) error {
	type Alias JobParams
	tmp := &struct {
		ScanStartTime string `json:"start_time"`
		*Alias
	}{
		Alias: (*Alias)(jp),
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	parsedDate, err := time.Parse("2006-01-02 15:04:05 MST", tmp.ScanStartTime)
	if err != nil {
		return err
	}

	jp.ScanStartTime = parsedDate

	if jp.ScanID == "" {
		jp.ScanID = jp.CheckID
	}

	return nil
}

// Job stores the information for a running job. It holds its own logger,
// context and cancel function. The BackendID field contains the backend
// specific id of the check.
type Job struct {
	JobParams
	log       Logger
	BackendID string
	Ctx       context.Context    `json:"-"`
	Cancel    context.CancelFunc `json:"-"`
}
