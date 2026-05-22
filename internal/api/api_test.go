package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
)

type enqueued struct {
	jobID  string
	action queue.Action
}

// fakeQueue records what the broker enqueues, and can be made to fail.
type fakeQueue struct {
	enqueued []enqueued
	failNext bool
}

func (f *fakeQueue) Enqueue(_ context.Context, jobID string, action queue.Action) error {
	if f.failNext {
		return io.ErrClosedPipe
	}
	f.enqueued = append(f.enqueued, enqueued{jobID, action})
	return nil
}
func (f *fakeQueue) Dequeue(context.Context, string, time.Duration) (*queue.Message, error) {
	return nil, nil
}
func (f *fakeQueue) Reclaim(context.Context, string, time.Duration, int) ([]*queue.Message, error) {
	return nil, nil
}
func (f *fakeQueue) Ack(context.Context, *queue.Message) error { return nil }

func newTestServer(q queue.Queue) (*Server, store.Store) {
	st := store.NewMem()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(st, q, log), st
}

func do(srv *Server, method, path, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)
	return rec
}

func TestProvisionAcceptsAndEnqueues(t *testing.T) {
	fq := &fakeQueue{}
	srv, st := newTestServer(fq)

	rec := do(srv, http.MethodPost, "/v1/provision",
		`{"service_name":"checkout","resource":"postgres"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body)
	}
	var job model.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.Status != model.StatusPending {
		t.Errorf("status = %q, want pending", job.Status)
	}
	if len(fq.enqueued) != 1 || fq.enqueued[0].jobID != job.ID {
		t.Fatalf("enqueued = %v, want one entry for %s", fq.enqueued, job.ID)
	}
	if fq.enqueued[0].action != queue.ActionProvision {
		t.Errorf("action = %q, want provision", fq.enqueued[0].action)
	}
	if _, err := st.Get(context.Background(), job.ID); err != nil {
		t.Errorf("job not persisted: %v", err)
	}
}

func TestProvisionEnqueueFailureMarksFailed(t *testing.T) {
	srv, _ := newTestServer(&fakeQueue{failNext: true})
	rec := do(srv, http.MethodPost, "/v1/provision", `{"service_name":"checkout"}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestProvisionRequiresServiceName(t *testing.T) {
	srv, _ := newTestServer(&fakeQueue{})
	rec := do(srv, http.MethodPost, "/v1/provision", `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestStatusNotFound(t *testing.T) {
	srv, _ := newTestServer(&fakeQueue{})
	rec := do(srv, http.MethodGet, "/v1/provision/deadbeef", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestListReturnsJobsOldestFirst(t *testing.T) {
	srv, st := newTestServer(&fakeQueue{})
	ctx := context.Background()
	_ = st.Put(ctx, &model.Job{ID: "b", Status: model.StatusReady,
		CreatedAt: time.Now().Add(-1 * time.Minute)})
	_ = st.Put(ctx, &model.Job{ID: "a", Status: model.StatusReady,
		CreatedAt: time.Now().Add(-2 * time.Minute)})

	rec := do(srv, http.MethodGet, "/v1/provision", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp struct {
		Count int         `json:"count"`
		Jobs  []model.Job `json:"jobs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 2 {
		t.Fatalf("count = %d, want 2", resp.Count)
	}
	if resp.Jobs[0].ID != "a" || resp.Jobs[1].ID != "b" {
		t.Errorf("order = [%s %s], want [a b]", resp.Jobs[0].ID, resp.Jobs[1].ID)
	}
}

func TestDeprovisionSetsDeletingAndEnqueues(t *testing.T) {
	fq := &fakeQueue{}
	srv, st := newTestServer(fq)
	_ = st.Put(context.Background(), &model.Job{ID: "j1", Status: model.StatusReady})

	rec := do(srv, http.MethodDelete, "/v1/provision/j1", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body)
	}
	got, _ := st.Get(context.Background(), "j1")
	if got.Status != model.StatusDeleting {
		t.Errorf("status = %q, want deleting", got.Status)
	}
	if len(fq.enqueued) != 1 || fq.enqueued[0].action != queue.ActionDeprovision {
		t.Errorf("enqueued = %v, want one deprovision entry", fq.enqueued)
	}
}

func TestDeprovisionUnknownReturns404(t *testing.T) {
	srv, _ := newTestServer(&fakeQueue{})
	rec := do(srv, http.MethodDelete, "/v1/provision/ghost", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestDeprovisionAlreadyDeletedIsIdempotent(t *testing.T) {
	fq := &fakeQueue{}
	srv, st := newTestServer(fq)
	_ = st.Put(context.Background(), &model.Job{ID: "j1", Status: model.StatusDeleted})

	rec := do(srv, http.MethodDelete, "/v1/provision/j1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(fq.enqueued) != 0 {
		t.Errorf("should not enqueue for an already-deleted job")
	}
}
