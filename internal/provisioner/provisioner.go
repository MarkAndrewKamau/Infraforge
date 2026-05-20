// Package provisioner turns a Job into a real running resource.
//
// The interface is intentionally tiny: the worker depends on Provision()
// and nothing else, so tests can substitute a fake without dragging in
// Docker. The shell-out Docker implementation lives in docker.go; a
// future Kubernetes or Docker-SDK implementation would simply implement
// Provisioner too.
package provisioner

import (
	"context"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

type Provisioner interface {
	Provision(ctx context.Context, j *model.Job) (*model.ConnectionInfo, error)
}
