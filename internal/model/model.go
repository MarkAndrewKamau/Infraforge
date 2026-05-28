// Package model holds the types shared across the broker, queue and worker.
// Keeping them in one place means the wire contract (JSON) and the internal
// state machine never drift apart.
package model

import "time"

// Status is the lifecycle of a single provisioning job.
//
//	pending ──▶ provisioning ──▶ ready ──▶ deleting ──▶ deleted
//	                  │
//	                  └────────▶ failed
type Status string

const (
	StatusPending      Status = "pending"
	StatusProvisioning Status = "provisioning"
	StatusReady        Status = "ready"
	StatusFailed       Status = "failed"
	StatusDeleting     Status = "deleting"
	StatusDeleted      Status = "deleted"
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
	// Attempts counts how many times a worker has begun provisioning this
	// job. It is persisted on the job so the count survives the very
	// crash that incremented it, which is what makes the poison-message
	// cap in the worker reliable.
	Attempts int `json:"attempts,omitempty"`
	// Connection is the Postgres endpoint, populated once Status == ready
	// and cleared again on deprovision.
	Connection *ConnectionInfo `json:"connection,omitempty"`
	// HTTP is the companion microservice endpoint, populated alongside
	// Connection. It is the routable workload for L7 routing.
	HTTP      *HTTPEndpoint `json:"http,omitempty"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// ConnectionInfo is how a caller reaches a provisioned database.
type ConnectionInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// HTTPEndpoint is how a caller reaches the companion HTTP microservice.
// It carries no credentials: the service is unauthenticated and bound to
// loopback on the worker's host.
type HTTPEndpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}
