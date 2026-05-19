// Package model holds the types shared across the broker, queue and worker.
// Keeping them in one place means the wire contract (JSON) and the internal
// state machine never drift apart.
package model

import "time"

// Status is the lifecycle of a single provisioning job.
//
//	pending ──▶ provisioning ──▶ ready
//	                  │
//	                  └────────▶ failed
type Status string

const (
	StatusPending      Status = "pending"
	StatusProvisioning Status = "provisioning"
	StatusReady        Status = "ready"
	StatusFailed       Status = "failed"
)

// ResourceType is the kind of thing a caller can ask the broker to provision.
// For now we only support Postgres; more types slot in here later.
type ResourceType string

const (
	ResourcePostgres ResourceType = "postgres"
)

// ProvisionRequest is the payload a client POSTs to ask for a resource.
type ProvisionRequest struct {
	ServiceName string       `json:"service_name"`
	Resource    ResourceType `json:"resource"`
}

// Job is the broker's record of one provisioning request and its lifecycle.
// It is returned verbatim (as JSON) from the status endpoint.
type Job struct {
	ID          string       `json:"id"`
	ServiceName string       `json:"service_name"`
	Resource    ResourceType `json:"resource"`
	Status      Status       `json:"status"`
	// Detail is a human-readable note about the current state, e.g. an
	// error message when Status == failed.
	Detail string `json:"detail,omitempty"`
	// Connection is populated once Status == ready. The worker fills this
	// in from Phase 3 onward.
	Connection *ConnectionInfo `json:"connection,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// ConnectionInfo is how a caller reaches a provisioned resource.
type ConnectionInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}
