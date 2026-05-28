package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/MarkAndrewKamau/infraforge/internal/provisioner"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
)

type fakeQueue struct{ acked []string }

func (f *fakeQueue) Enqueue(context.Context, string, queue.Action) error { return nil }
func (f *fakeQueue) Dequeue(context.Context, string, time.Duration) (*queue.Message, error) {
	return nil, nil
}
func (f *fakeQueue) Reclaim(context.Context, string, time.Duration, int) ([]*queue.Message, error) {
	return nil, nil
}
func (f *fakeQueue) Ack(_ context.Context, m *queue.Message) error {
	f.acked = append(f.acked, m.ID)
	return nil
}

type fakeProv struct {
	result        *provisioner.Result
	provErr       error
	deprovErr     error
	provisioned   []string
	deprovisioned []string
}

func (f *fakeProv) Provision(_ context.Context, j *model.Job) (*provisioner.Result, error) {
	f.provisioned = append(f.provisioned, j.ID)
	return f.result, f.provErr
}
func (f *fakeProv) Deprovision(_ context.Context, j *model.Job) error {
	f.deprovisioned = append(f.deprovisioned, j.ID)
	return f.deprovErr
}

func newTestWorker(prov *fakeProv, q *fakeQueue) (*Worker, store.Store) {
	st := store.NewMem()
	w := &Worker{
		Name:        "test",
		Store:       st,
		Queue:       q,
		Provision:   prov,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxAttempts: 3,
	}
	return w, st
}

func seed(t *testing.T, st store.Store, j *model.Job) {
	t.Helper()
	if err := st.Put(context.Background(), j); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func provMsg(jobID string) *queue.Message {
	return &queue.Message{ID: "s-" + jobID, JobID: jobID, Action: queue.ActionProvision}
}
func deprovMsg(jobID string) *queue.Message {
	return &queue.Message{ID: "s-" + jobID, JobID: jobID, Action: queue.ActionDeprovision}
}

func TestHandleProvisionSuccess(t *testing.T) {
	q := &fakeQueue{}
	res := &provisioner.Result{
		Connection: &model.ConnectionInfo{Host: "127.0.0.1", Port: 54321, Database: "d"},
		HTTP:       &model.HTTPEndpoint{Host: "127.0.0.1", Port: 18080},
	}
	w, st := newTestWorker(&fakeProv{result: res}, q)
	seed(t, st, &model.Job{ID: "j1", ServiceName: "checkout", Status: model.StatusPending})

	w.handle(context.Background(), provMsg("j1"))

	got, err := st.Get(context.Background(), "j1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != model.StatusReady {
		t.Errorf("status = %s, want ready", got.Status)
	}
	if got.Connection == nil || got.Connection.Port != 54321 {
		t.Errorf("connection = %+v", got.Connection)
	}
	if got.HTTP == nil || got.HTTP.Port != 18080 {
		t.Errorf("http = %+v, want port 18080", got.HTTP)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", got.Attempts)
	}
	if len(q.acked) != 1 {
		t.Errorf("acked = %v, want one", q.acked)
	}
}

func TestHandleProvisionFailureMarksFailed(t *testing.T) {
	q := &fakeQueue{}
	w, st := newTestWorker(&fakeProv{provErr: errors.New("docker exploded")}, q)
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusPending})

	w.handle(context.Background(), provMsg("j1"))

	got, _ := st.Get(context.Background(), "j1")
	if got.Status != model.StatusFailed {
		t.Errorf("status = %s, want failed", got.Status)
	}
	if got.Detail == "" {
		t.Errorf("want detail set on failure")
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack on failure")
	}
}

func TestHandleProvisionMissingJobAcks(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{}
	w, _ := newTestWorker(prov, q)

	w.handle(context.Background(), provMsg("ghost"))

	if len(q.acked) != 1 {
		t.Errorf("expected ack for missing job")
	}
	if len(prov.provisioned) != 0 {
		t.Errorf("provisioner should not run for a missing job")
	}
}

func TestHandleProvisionAlreadyReadyIsIdempotent(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{}
	w, st := newTestWorker(prov, q)
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusReady})

	w.handle(context.Background(), provMsg("j1"))

	if len(prov.provisioned) != 0 {
		t.Errorf("provisioner should not re-run for a ready job")
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack for redelivered ready job")
	}
}

func TestHandleProvisionAbandonsAfterMaxAttempts(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{result: &provisioner.Result{
		Connection: &model.ConnectionInfo{},
		HTTP:       &model.HTTPEndpoint{},
	}}
	w, st := newTestWorker(prov, q)
	// The job has already been attempted MaxAttempts times: it crashed
	// the worker each time and kept getting reclaimed.
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusProvisioning, Attempts: 3})

	w.handle(context.Background(), provMsg("j1"))

	got, _ := st.Get(context.Background(), "j1")
	if got.Status != model.StatusFailed {
		t.Errorf("status = %s, want failed", got.Status)
	}
	if len(prov.provisioned) != 0 {
		t.Errorf("provisioner should not run past the attempt cap")
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack when abandoning a poison job")
	}
}

func TestHandleDeprovisionSuccess(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{}
	w, st := newTestWorker(prov, q)
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusDeleting,
		Connection: &model.ConnectionInfo{Port: 1},
		HTTP:       &model.HTTPEndpoint{Port: 2}})

	w.handle(context.Background(), deprovMsg("j1"))

	got, _ := st.Get(context.Background(), "j1")
	if got.Status != model.StatusDeleted {
		t.Errorf("status = %s, want deleted", got.Status)
	}
	if got.Connection != nil {
		t.Errorf("connection should be cleared on deprovision")
	}
	if got.HTTP != nil {
		t.Errorf("http endpoint should be cleared on deprovision")
	}
	if len(prov.deprovisioned) != 1 {
		t.Errorf("deprovisioner should have run once, ran %d", len(prov.deprovisioned))
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack")
	}
}

func TestHandleDeprovisionFailureKeepsDeleting(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{deprovErr: errors.New("rm failed")}
	w, st := newTestWorker(prov, q)
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusDeleting})

	w.handle(context.Background(), deprovMsg("j1"))

	got, _ := st.Get(context.Background(), "j1")
	if got.Status != model.StatusDeleting {
		t.Errorf("status = %s, want deleting (unchanged)", got.Status)
	}
	if got.Detail == "" {
		t.Errorf("want failure detail recorded")
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack to avoid an endless retry loop")
	}
}
