package stateupdater

// CheckState defines the all the possible fields of the states
// sent to the check state queue.
type CheckState struct {
	ID       string   `json:"id" validate:"required"`
	Status   *string  `json:"status" validate:"required"`
	ScanID   *string  `json:"scan_id" validate:"required"`
	AgentID  *string  `json:"agent_id" validate:"required"`
	Report   *string  `json:"report,omitempty"`
	Raw      *string  `json:"raw,omitempty"`
	Progress *float32 `json:"progress,omitempty"`
}
