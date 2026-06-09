package xds

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/MarkAndrewKamau/infraforge/internal/model"

	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
)

type fakeBroker struct {
	jobs []model.Job
	err  error
	// calls counts how many times List was invoked, so tests can assert
	// "we only called the broker once."
	calls int
}

func (f *fakeBroker) List(context.Context) ([]model.Job, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.jobs, nil
}

func newReconciler(b *fakeBroker) (*Reconciler, *Registry) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := cachev3.NewSnapshotCache(false, cachev3.IDHash{}, nil)
	reg := NewRegistry(cache, log)
	return &Reconciler{Broker: b, Registry: reg, Log: log}, reg
}

func TestReconcileFiltersToReadyJobsWithHTTP(t *testing.T) {
	b := &fakeBroker{jobs: []model.Job{
		// Should appear
		{ID: "j1", ServiceName: "checkout", Status: model.StatusReady,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 1001}},
		{ID: "j2", ServiceName: "checkout", Status: model.StatusReady,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 1002}},
		{ID: "j3", ServiceName: "billing", Status: model.StatusReady,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 1003}},
		// Should NOT appear
		{ID: "p1", ServiceName: "x", Status: model.StatusPending},
		{ID: "p2", ServiceName: "x", Status: model.StatusProvisioning,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 999}},
		{ID: "p3", ServiceName: "x", Status: model.StatusFailed},
		{ID: "p4", ServiceName: "x", Status: model.StatusDeleted,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 999}},
		{ID: "p5", ServiceName: "x", Status: model.StatusReady, HTTP: nil}, // weird; ready with no http
	}}
	r, reg := newReconciler(b)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := reg.List()
	if len(got) != 2 {
		t.Fatalf("services = %v, want 2 (checkout, billing)", got)
	}
	if len(got["checkout"]) != 2 {
		t.Errorf("checkout endpoints = %v, want 2", got["checkout"])
	}
	if len(got["billing"]) != 1 {
		t.Errorf("billing endpoints = %v, want 1", got["billing"])
	}
}

func TestReconcileIsIdempotentWhenStateUnchanged(t *testing.T) {
	b := &fakeBroker{jobs: []model.Job{
		{ID: "j1", ServiceName: "checkout", Status: model.StatusReady,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 1001}},
	}}
	r, reg := newReconciler(b)

	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// A second reconcile against the same broker state must change
	// nothing — ReplaceAll should detect equality and return changed=false.
	changed, err := reg.ReplaceAll(context.Background(),
		map[string][]Endpoint{
			"checkout": {{Host: "127.0.0.1", Port: 1001}},
		})
	if err != nil {
		t.Fatalf("ReplaceAll: %v", err)
	}
	if changed {
		t.Errorf("ReplaceAll claimed changed=true on identical state")
	}
}

func TestReconcileRemovesStaleEndpoints(t *testing.T) {
	b := &fakeBroker{jobs: []model.Job{
		{ID: "j1", ServiceName: "checkout", Status: model.StatusReady,
			HTTP: &model.HTTPEndpoint{Host: "127.0.0.1", Port: 1001}},
	}}
	r, reg := newReconciler(b)
	// Pre-populate the registry with something the broker no longer knows
	// about, simulating drift (e.g. an endpoint that disappeared during
	// an xds outage).
	if err := reg.Register(context.Background(), "stale", Endpoint{Host: "127.0.0.1", Port: 9999}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := reg.List()
	if _, found := got["stale"]; found {
		t.Errorf("stale service should have been removed, got %v", got)
	}
	if len(got["checkout"]) != 1 {
		t.Errorf("checkout endpoints = %v, want 1", got["checkout"])
	}
}

func TestReconcileBrokerErrorBubblesUp(t *testing.T) {
	b := &fakeBroker{err: errors.New("connection refused")}
	r, _ := newReconciler(b)

	err := r.Reconcile(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
