// Package store keeps the state of every provisioning job.
//
// Store is an interface so the broker and worker can share state through
// Redis (RedisStore) while tests and standalone runs use a process-local
// map (MemStore). The API layer depends only on this interface, so the
// backing swap is invisible above this package.
package store

import (
	"context"
	"errors"
	"sync"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

// ErrNotFound is returned when a job ID is unknown.
var ErrNotFound = errors.New("job not found")

type Store interface {
	// Put inserts or overwrites a job.
	Put(ctx context.Context, j *model.Job) error
	// Get returns the job, or ErrNotFound.
	Get(ctx context.Context, id string) (*model.Job, error)
}

// MemStore is a concurrency-safe in-memory Store.
type MemStore struct {
	mu   sync.RWMutex
	jobs map[string]*model.Job
}

func NewMem() *MemStore {
	return &MemStore{jobs: make(map[string]*model.Job)}
}

func (s *MemStore) Put(_ context.Context, j *model.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
	return nil
}

func (s *MemStore) Get(_ context.Context, id string) (*model.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}
