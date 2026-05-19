// Package api is the broker's HTTP surface — the "control plane" entrypoint.
//
// The key design idea: provisioning is slow (pulling images, starting
// containers), so the API never does it inline. POST returns 202 Accepted
// with a job ID immediately; the caller polls GET for the outcome. From
// Phase 2 the actual work happens in a separate worker process.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
)

type Server struct {
	store store.Store
	queue queue.Queue
	log   *slog.Logger
}

func NewServer(st store.Store, q queue.Queue, log *slog.Logger) *Server {
	return &Server{store: st, queue: q, log: log}
}

// Routes wires the method+path patterns (Go 1.22+ ServeMux) to handlers.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /v1/provision", s.handleProvision)
	mux.HandleFunc("GET /v1/provision/{id}", s.handleStatus)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req model.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.ServiceName == "" {
		writeError(w, http.StatusBadRequest, "service_name is required")
		return
	}
	if req.Resource == "" {
		req.Resource = model.ResourcePostgres // sensible default
	}
	if req.Resource != model.ResourcePostgres {
		writeError(w, http.StatusBadRequest, "unsupported resource: "+string(req.Resource))
		return
	}

	ctx := r.Context()
	now := time.Now().UTC()
	job := &model.Job{
		ID:          newID(),
		ServiceName: req.ServiceName,
		Resource:    req.Resource,
		Status:      model.StatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Persist the job before enqueuing. If we enqueued first and the store
	// write failed, a worker could pick up a job that has no state record.
	if err := s.store.Put(ctx, job); err != nil {
		s.log.Error("store put failed", "id", job.ID, "err", err)
		writeError(w, http.StatusInternalServerError, "could not record job")
		return
	}

	// Hand the job to the worker via the queue. If this fails the job is
	// orphaned (no worker will ever see it), so mark it failed and tell
	// the caller — never return 202 for work nothing will pick up.
	if err := s.queue.Enqueue(ctx, job.ID); err != nil {
		s.log.Error("enqueue failed", "id", job.ID, "err", err)
		job.Status = model.StatusFailed
		job.Detail = "could not enqueue provisioning job"
		job.UpdatedAt = time.Now().UTC()
		_ = s.store.Put(ctx, job)
		writeError(w, http.StatusInternalServerError, "could not enqueue job")
		return
	}

	s.log.Info("provision requested",
		"id", job.ID, "service", job.ServiceName, "resource", job.Resource)
	w.Header().Set("Location", "/v1/provision/"+job.ID)
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no job with id "+id)
		return
	}
	if err != nil {
		s.log.Error("store get failed", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not read job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// newID is a short random hex ID — enough for a local learning system.
func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
