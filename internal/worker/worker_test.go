package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
)

type fakeQueue struct{ acked []string }

func (f *fakeQueue) Enqueue(context.Context, string) error { return nil }
func (f *fakeQueue) Dequeue(context.Context, string, time.Duration) (*queue.Message, error) {
	return nil, nil
}
func (f *fakeQueue) Ack(_ context.Context, m *queue.Message) error {
	f.acked = append(f.acked, m.ID)
	return nil
}

type fakeProv struct {
	conn *model.ConnectionInfo
	err  error
	// records the job ids we were asked to provision
	seen []string
}

func (f *fakeProv) Provision(_ context.Context, j *model.Job) (*model.ConnectionInfo, error) {
	f.seen = append(f.seen, j.ID)
	return f.conn, f.err
}

func newTestWorker(prov *fakeProv, q *fakeQueue) (*Worker, store.Store) {
	st := store.NewMem()
	return &Worker{
		Name:      "test",
		Store:     st,
		Queue:     q,
		Provision: prov,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, st
}

func seed(t *testing.T, st store.Store, j *model.Job) {
	t.Helper()
	if err := st.Put(context.Background(), j); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestHandleSuccess(t *testing.T) {
	q := &fakeQueue{}
	conn := &model.ConnectionInfo{Host: "127.0.0.1", Port: 54321, Username: "u", Password: "p", Database: "d"}
	w, st := newTestWorker(&fakeProv{conn: conn}, q)
	seed(t, st, &model.Job{ID: "j1", ServiceName: "checkout", Status: model.StatusPending})

	w.handle(context.Background(), &queue.Message{ID: "s1", JobID: "j1"})

	got, err := st.Get(context.Background(), "j1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != model.StatusReady {
		t.Errorf("status = %s, want ready", got.Status)
	}
	if got.Connection == nil || got.Connection.Port != 54321 {
		t.Errorf("connection = %+v, want port 54321", got.Connection)
	}
	if len(q.acked) != 1 || q.acked[0] != "s1" {
		t.Errorf("acked = %v, want [s1]", q.acked)
	}
}

func TestHandleProvisionerFailureMarksFailed(t *testing.T) {
	q := &fakeQueue{}
	w, st := newTestWorker(&fakeProv{err: errors.New("docker exploded")}, q)
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusPending})

	w.handle(context.Background(), &queue.Message{ID: "s1", JobID: "j1"})

	got, _ := st.Get(context.Background(), "j1")
	if got.Status != model.StatusFailed {
		t.Errorf("status = %s, want failed", got.Status)
	}
	if got.Detail == "" {
		t.Errorf("want detail set on failure")
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack even on failure, got %v", q.acked)
	}
}

func TestHandleMissingJobAcksAndDrops(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{}
	w, _ := newTestWorker(prov, q)
	w.handle(context.Background(), &queue.Message{ID: "s1", JobID: "ghost"})
	if len(q.acked) != 1 {
		t.Errorf("expected ack for missing job, got %v", q.acked)
	}
	if len(prov.seen) != 0 {
		t.Errorf("provisioner should not have been called for missing job")
	}
}

func TestHandleAlreadyReadyIsIdempotent(t *testing.T) {
	q := &fakeQueue{}
	prov := &fakeProv{}
	w, st := newTestWorker(prov, q)
	seed(t, st, &model.Job{ID: "j1", Status: model.StatusReady})

	w.handle(context.Background(), &queue.Message{ID: "s1", JobID: "j1"})

	if len(prov.seen) != 0 {
		t.Errorf("provisioner should not run again for ready job, got %v", prov.seen)
	}
	if len(q.acked) != 1 {
		t.Errorf("expected ack for redelivered ready job")
	}
}
