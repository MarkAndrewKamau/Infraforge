package xds

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

// JobLister is the slice of the broker client the reconciler depends
// on. Defining it locally (rather than importing brokerclient.Client
// directly) keeps this package free of an HTTP dependency, which makes
// the reconciler trivially testable with a fake.
type JobLister interface {
	List(ctx context.Context) ([]model.Job, error)
}

// Reconciler walks the broker's job inventory and replaces the xDS
// registry with what should be there. It is the safety net for
// situations the per-event Register/Unregister path cannot cover:
//
//   - cmd/xds restarts and loses its in-memory registry
//   - the worker registered while cmd/xds was unreachable (warning logged,
//     no retry) and the registry now lags reality
//   - any other drift between the broker store and the registry
//
// The pattern is the Kubernetes controller pattern: treat events as a
// fast path, treat periodic reconciliation against the source of truth
// as the safety net.
type Reconciler struct {
	Broker   JobLister
	Registry *Registry
	Log      *slog.Logger
}

// Reconcile fetches the current job list and replaces the registry. A
// reconcile that finds the registry already correct is a cheap no-op
// (no version bump, no Envoy push).
func (r *Reconciler) Reconcile(ctx context.Context) error {
	jobs, err := r.Broker.List(ctx)
	if err != nil {
		return fmt.Errorf("list jobs: %w", err)
	}
	desired := buildDesired(jobs)
	changed, err := r.Registry.ReplaceAll(ctx, desired)
	if err != nil {
		return fmt.Errorf("replace registry: %w", err)
	}
	if changed {
		r.Log.Info("xds reconciled from broker",
			"services", len(desired), "endpoints", countEndpoints(desired))
	}
	return nil
}

// Run executes one reconcile immediately, then on every tick of every
// until ctx is done. every == 0 disables the periodic tick (initial
// reconcile only).
func (r *Reconciler) Run(ctx context.Context, every time.Duration) {
	if err := r.Reconcile(ctx); err != nil {
		// First reconcile failing is not fatal: Envoy will keep the
		// empty (or stale) snapshot it has and the next tick can try
		// again. Provisioning events from the worker still flow.
		r.Log.Warn("initial reconcile failed", "err", err)
	}
	if every == 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Reconcile(ctx); err != nil {
				r.Log.Warn("reconcile failed", "err", err)
			}
		}
	}
}

// buildDesired turns the broker's job list into the registry-shaped map.
// Only ready jobs with a populated HTTP endpoint contribute — pending,
// provisioning, failed and deleted jobs have nothing routable.
func buildDesired(jobs []model.Job) map[string][]Endpoint {
	out := map[string][]Endpoint{}
	for _, j := range jobs {
		if j.Status != model.StatusReady || j.HTTP == nil {
			continue
		}
		out[j.ServiceName] = append(out[j.ServiceName], Endpoint{
			Host: j.HTTP.Host, Port: j.HTTP.Port,
		})
	}
	return out
}

func countEndpoints(m map[string][]Endpoint) int {
	n := 0
	for _, eps := range m {
		n += len(eps)
	}
	return n
}
