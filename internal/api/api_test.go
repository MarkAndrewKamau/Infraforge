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

// fakeQueue records what the broker enqueues, and can be made to fail.
type fakeQueue struct {
	enqueued []string
	failNext bool
}

func (f *fakeQueue) Enqueue(_ context.Context, jobID string) error {
	if f.failNext {
		return io.ErrClosedPipe
	}
	f.enqueued = append(f.enqueued, jobID)
	return nil
}
func (f *fakeQueue) Dequeue(context.Context, string, time.Duration) (*queue.Message, error) {
	return nil, nil
}
func (f *fakeQueue) Ack(context.Context, *queue.Message) error { return nil }

func newTestServer(q queue.Queue) (*Server, store.Store) {
	st := store.NewMem()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(st, q, log), st
}

func TestProvisionAcceptsAndEnqueues(t *testing.T) {
	fq := &fakeQueue{}
	srv, st := newTestServer(fq)

	body := strings.NewReader(`{"service_name":"checkout","resource":"postgres"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/provision", body)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

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
	if len(fq.enqueued) != 1 || fq.enqueued[0] != job.ID {
		t.Errorf("enqueued = %v, want [%s]", fq.enqueued, job.ID)
	}
	if _, err := st.Get(context.Background(), job.ID); err != nil {
		t.Errorf("job not persisted: %v", err)
	}
}

func TestProvisionEnqueueFailureMarksFailed(t *testing.T) {
	fq := &fakeQueue{failNext: true}
	srv, _ := newTestServer(fq)

	req := httptest.NewRequest(http.MethodPost, "/v1/provision",
		strings.NewReader(`{"service_name":"checkout"}`))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestProvisionRequiresServiceName(t *testing.T) {
	srv, _ := newTestServer(&fakeQueue{})
	req := httptest.NewRequest(http.MethodPost, "/v1/provision", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestStatusNotFound(t *testing.T) {
	srv, _ := newTestServer(&fakeQueue{})
	req := httptest.NewRequest(http.MethodGet, "/v1/provision/deadbeef", nil)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
