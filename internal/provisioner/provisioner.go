// Package provisioner turns a Job into a real running resource, and tears
// it down again.
//
// The interface is intentionally tiny: the worker depends on Provision()
// and Deprovision() and nothing else, so tests can substitute a fake
// without dragging in Docker. The shell-out Docker implementation lives
// in docker.go; a future Kubernetes or Docker-SDK implementation would
// simply implement Provisioner too.
package provisioner

import (
	"context"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

type Provisioner interface {
	// Provision brings the job's resource into existence and returns how
	// to reach it. It must be idempotent: called twice for the same job
	// it returns the same resource, not a duplicate.
	Provision(ctx context.Context, j *model.Job) (*model.ConnectionInfo, error)
	// Deprovision removes the job's resource. It must be idempotent: a
	// resource that is already gone is a success, not an error.
	Deprovision(ctx context.Context, j *model.Job) error
}
