// Package provisioner turns a Job into running resources, and tears
// them down again.
//
// A single job produces more than one container — currently a Postgres
// database and a companion HTTP microservice. Result carries both
// endpoints so the worker can persist them together; a future
// Kubernetes or SDK-based implementation would return the same shape.
package provisioner

import (
	"context"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

// Result is what Provision returns once a job's resources are running.
type Result struct {
	Connection *model.ConnectionInfo
	HTTP       *model.HTTPEndpoint
}

type Provisioner interface {
	// Provision brings the job's resources into existence and returns
	// how to reach them. It must be idempotent: called twice for the
	// same job it returns the same resources, not duplicates.
	Provision(ctx context.Context, j *model.Job) (*Result, error)
	// Deprovision removes every resource belonging to the job. It must
	// be idempotent: resources that are already gone are a success.
	Deprovision(ctx context.Context, j *model.Job) error
}
