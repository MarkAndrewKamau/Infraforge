// Package worker is the long-running consumer that turns queued jobs
// into real resources. It is the heart of the "background worker service"
// described in the project brief.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/MarkAndrewKamau/infraforge/internal/provisioner"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
)

type Worker struct {
	// Name is this worker's consumer identity inside the Redis stream
	// group. With multiple workers, each one needs a unique name so the
	// group can fan jobs out across them.
	Name      string
	Store     store.Store
	Queue     queue.Queue
	Provision provisioner.Provisioner
	Log       *slog.Logger
	// BlockTimeout is how long XREADGROUP blocks per iteration. Short
	// enough that ctx cancellation feels responsive; long enough to
	// avoid spinning Redis when the queue is idle.
	BlockTimeout time.Duration
}

// Run consumes the queue until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	if w.BlockTimeout == 0 {
		w.BlockTimeout = 5 * time.Second
	}
	w.Log.Info("worker started", "name", w.Name)
	for {
		if ctx.Err() != nil {
			w.Log.Info("worker stopped", "name", w.Name)
			return
		}
		msg, err := w.Queue.Dequeue(ctx, w.Name, w.BlockTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.Log.Error("dequeue failed", "err", err)
			sleep(ctx, time.Second)
			continue
		}
		if msg == nil {
			continue // block elapsed with nothing — just loop
		}
		w.handle(ctx, msg)
	}
}

func (w *Worker) handle(ctx context.Context, msg *queue.Message) {
	log := w.Log.With("job", msg.JobID, "stream_id", msg.ID)

	job, err := w.Store.Get(ctx, msg.JobID)
	if err != nil {
		// No state record for this id. Either it expired (24h TTL) or
		// was never persisted. Nothing we can do — ack so the dangling
		// reference doesn't sit in the pending list forever.
		log.Warn("job not in store, dropping", "err", err)
		_ = w.Queue.Ack(ctx, msg)
		return
	}
	if job.Status == model.StatusReady {
		// Already done — almost certainly a redelivery. Ack and move on.
		// This is the idempotency safety net for the persist-then-ack
		// ordering in the success path below.
		log.Info("job already ready, acking redelivery")
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	job.Status = model.StatusProvisioning
	job.Detail = ""
	job.UpdatedAt = time.Now().UTC()
	_ = w.Store.Put(ctx, job)

	conn, err := w.Provision.Provision(ctx, job)
	if err != nil {
		log.Error("provision failed", "err", err)
		job.Status = model.StatusFailed
		job.Detail = err.Error()
		job.UpdatedAt = time.Now().UTC()
		_ = w.Store.Put(ctx, job)
		// Phase 7 will distinguish retryable failures via XCLAIM /
		// dead-letter; for now we ack to keep the example tractable.
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	job.Status = model.StatusReady
	job.Connection = conn
	job.Detail = ""
	job.UpdatedAt = time.Now().UTC()
	_ = w.Store.Put(ctx, job)
	if err := w.Queue.Ack(ctx, msg); err != nil {
		// Ack failure here is recoverable: the state is already correct,
		// and the redelivery will hit the StatusReady early-return.
		log.Warn("ack failed after success", "err", err)
	}
	log.Info("provisioned",
		"host", conn.Host, "port", conn.Port, "db", conn.Database)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
