package deployer

import "time"

type Job struct {
	ID        string     `json:"id,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
	Steps     []Step     `json:"steps,omitempty"`
}

type Step struct {
	ReleaseID string   `json:"release_id,omitempty"`
	Cmd       []string `json:"cmd,omitempty"`
}
