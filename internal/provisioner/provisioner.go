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
	"errors"
	"fmt"
	"strings"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

// ErrTransient is wrapped around errors the worker should treat as
// retryable: the underlying cause (Docker daemon unreachable, network
// blip, etc.) may resolve on its own, and a later reclaim attempt could
// succeed. Anything not wrapped with ErrTransient is treated by the
// worker as permanent — retrying would produce the same error, so the
// job is marked failed and dead-lettered immediately.
//
// Default is permanent. We classify into transient on a small allowlist
// of well-known causes rather than guessing at unknown errors.
var ErrTransient = errors.New("transient error")

// Classify wraps err with ErrTransient if its message matches a known
// transient pattern. nil in, nil out.
func Classify(err error) error {
	if err == nil {
		return nil
	}
	s := strings.ToLower(err.Error())
	for _, p := range transientPatterns {
		if strings.Contains(s, p) {
			return fmt.Errorf("%w: %s", ErrTransient, err.Error())
		}
	}
	return err
}

// transientPatterns is the substring allowlist for transient
// classification. Lowercase for case-insensitive matching. Add patterns
// here when we identify new well-known transient causes.
var transientPatterns = []string{
	"cannot connect to the docker daemon",
	"context deadline exceeded",
	"i/o timeout",
	"connection refused",
	"network is unreachable",
	"no such host",
	"temporary failure in name resolution",
	"docker daemon",
}

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
