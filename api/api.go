package api

import (
	report "github.com/adevinta/vulcan-report"
)

// CheckState holds the values related to the state of a check.
// The values defined here the one written to the check states quewue
type CheckState struct {
	Report   *report.Report `json:"report,omitempty"`
	Progress *float32       `json:"progress,omitempty"`
	Status   *string        `json:"status,omitempty"`
}
