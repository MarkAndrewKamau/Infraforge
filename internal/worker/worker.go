// Package worker is the long-running consumer that turns queued jobs into
// real resources, and tears them down again. It is the "background worker
// service" described in the project brief.
//
// The worker runs two loops. The main loop reads brand-new messages with
// XREADGROUP ">". The reclaim loop periodically runs XAUTOCLAIM to rescue
// messages that were delivered to a worker which then died before acking
// them — a fresh XREADGROUP ">" would never see those again. Together
// they make a crash mid-provision recoverable rather than fatal.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
	"github.com/MarkAndrewKamau/infraforge/internal/provisioner"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
	"github.com/MarkAndrewKamau/infraforge/internal/xdsclient"
)

const (
	defaultBlockTimeout   = 5 * time.Second
	defaultReclaimEvery   = 30 * time.Second
	defaultReclaimMinIdle = 2 * time.Minute
	defaultMaxAttempts    = 3
)

type Worker struct {
	// Name is this worker's consumer identity inside the Redis stream
	// group. Each running worker needs a unique name so the group can fan
	// jobs out across them — and so XAUTOCLAIM can tell whose pending
	// entries are whose.
	Name      string
	Store     store.Store
	Queue     queue.Queue
	Provision provisioner.Provisioner
	// XDS is notified after each successful provision and before each
	// deprovision so Envoy's live routing follows the resource state.
	// xdsclient.Noop is a valid value when no control plane is running.
	XDS xdsclient.Client
	Log *slog.Logger

	// BlockTimeout is how long XREADGROUP blocks per main-loop iteration.
	BlockTimeout time.Duration
	// ReclaimEvery is how often the reclaim loop runs.
	ReclaimEvery time.Duration
	// ReclaimMinIdle is how long a delivered-but-unacked message must sit
	// idle before the reclaim loop assumes its worker died and takes it
	// over. It must comfortably exceed the slowest healthy provisioning
	// time, or a slow-but-alive provision could be stolen mid-flight.
	ReclaimMinIdle time.Duration
	// MaxAttempts caps how many times a single job may be (re)tried
	// before it is abandoned as a poison message.
	MaxAttempts int
}

func (w *Worker) applyDefaults() {
	if w.BlockTimeout == 0 {
		w.BlockTimeout = defaultBlockTimeout
	}
	if w.ReclaimEvery == 0 {
		w.ReclaimEvery = defaultReclaimEvery
	}
	if w.ReclaimMinIdle == 0 {
		w.ReclaimMinIdle = defaultReclaimMinIdle
	}
	if w.MaxAttempts == 0 {
		w.MaxAttempts = defaultMaxAttempts
	}
	if w.XDS == nil {
		w.XDS = xdsclient.Noop{}
	}
}

// Run consumes the queue until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.applyDefaults()
	w.Log.Info("worker started",
		"name", w.Name,
		"reclaim_every", w.ReclaimEvery,
		"reclaim_min_idle", w.ReclaimMinIdle,
		"max_attempts", w.MaxAttempts)

	go w.reclaimLoop(ctx)

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

// reclaimLoop is the crash-recovery path. On each tick it asks Redis for
// messages that have been pending longer than ReclaimMinIdle, takes
// ownership of them, and runs them through the same handler as fresh
// work. Because handle is idempotent (deterministic resource names plus
// the status checks below), reprocessing a half-finished job is safe.
func (w *Worker) reclaimLoop(ctx context.Context) {
	t := time.NewTicker(w.ReclaimEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			msgs, err := w.Queue.Reclaim(ctx, w.Name, w.ReclaimMinIdle, 16)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.Log.Error("reclaim failed", "err", err)
				continue
			}
			for _, msg := range msgs {
				if ctx.Err() != nil {
					return
				}
				w.Log.Warn("reclaimed stale message",
					"job", msg.JobID, "stream_id", msg.ID, "action", msg.Action)
				w.handle(ctx, msg)
			}
		}
	}
}

func (w *Worker) handle(ctx context.Context, msg *queue.Message) {
	log := w.Log.With("job", msg.JobID, "stream_id", msg.ID, "action", msg.Action)

	job, err := w.Store.Get(ctx, msg.JobID)
	if err != nil {
		// No state record for this id. Nothing we can do — ack so the
		// dangling reference doesn't sit in the pending list forever.
		log.Warn("job not in store, dropping", "err", err)
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	switch msg.Action {
	case queue.ActionDeprovision:
		w.deprovision(ctx, log, msg, job)
	default:
		w.provision(ctx, log, msg, job)
	}
}

func (w *Worker) provision(ctx context.Context, log *slog.Logger, msg *queue.Message, job *model.Job) {
	switch job.Status {
	case model.StatusReady:
		// Already done — almost certainly a redelivery. This is the
		// idempotency safety net for the persist-then-ack ordering in
		// the success path below.
		log.Info("job already ready, acking redelivery")
		_ = w.Queue.Ack(ctx, msg)
		return
	case model.StatusDeleting, model.StatusDeleted:
		// A deprovision overtook this provision. Do not resurrect it.
		log.Info("job is being torn down, skipping provision")
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	// Poison-message guard. A job that keeps crashing its worker would
	// otherwise be reclaimed and retried forever. Attempts is persisted
	// on the job, so the count survives the crash that incremented it.
	job.Attempts++
	if job.Attempts > w.MaxAttempts {
		log.Error("abandoning job: too many attempts", "attempts", job.Attempts-1)
		job.Status = model.StatusFailed
		job.Detail = fmt.Sprintf("provisioning abandoned after %d attempts", job.Attempts-1)
		job.UpdatedAt = now()
		_ = w.Store.Put(ctx, job)
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	job.Status = model.StatusProvisioning
	job.Detail = ""
	job.UpdatedAt = now()
	_ = w.Store.Put(ctx, job)

	res, err := w.Provision.Provision(ctx, job)
	if err != nil {
		log.Error("provision failed", "err", err)
		job.Status = model.StatusFailed
		job.Detail = err.Error()
		job.UpdatedAt = now()
		_ = w.Store.Put(ctx, job)
		// A returned error is a definite, observed failure (bad config,
		// missing image). Retrying it would just fail the same way, so
		// we ack. Only *crashes* — where the worker dies and no error is
		// ever returned — are retried, via the reclaim loop.
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	job.Status = model.StatusReady
	job.Connection = res.Connection
	job.HTTP = res.HTTP
	job.Detail = ""
	job.UpdatedAt = now()
	_ = w.Store.Put(ctx, job)

	// Tell the xDS control plane about the new HTTP endpoint so Envoy
	// can route to it live. Failure is a warning, not a job failure: the
	// container is real and reachable directly via its port; only routing
	// through Envoy is temporarily missing.
	if err := w.XDS.Register(ctx, job.ServiceName, res.HTTP.Host, res.HTTP.Port); err != nil {
		log.Warn("xds register failed; resource is up but Envoy will not route to it",
			"err", err)
	}

	if err := w.Queue.Ack(ctx, msg); err != nil {
		// Ack failure here is recoverable: the state is already correct,
		// and any redelivery hits the StatusReady early-return above.
		log.Warn("ack failed after success", "err", err)
	}
	log.Info("provisioned",
		"db_host", res.Connection.Host, "db_port", res.Connection.Port,
		"db", res.Connection.Database,
		"http_host", res.HTTP.Host, "http_port", res.HTTP.Port)
}

func (w *Worker) deprovision(ctx context.Context, log *slog.Logger, msg *queue.Message, job *model.Job) {
	if job.Status == model.StatusDeleted {
		log.Info("job already deleted, acking redelivery")
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	// Unregister from xDS first so Envoy stops sending new traffic to a
	// backend about to disappear. Failure is a warning only — teardown
	// proceeds either way; a stale cluster entry without endpoints is
	// harmless beyond a 503 from Envoy.
	if job.HTTP != nil {
		if err := w.XDS.Unregister(ctx, job.ServiceName, job.HTTP.Host, job.HTTP.Port); err != nil {
			log.Warn("xds unregister failed; proceeding with teardown", "err", err)
		}
	}

	if err := w.Provision.Deprovision(ctx, job); err != nil {
		log.Error("deprovision failed", "err", err)
		job.Detail = "deprovision failed: " + err.Error()
		job.UpdatedAt = now()
		_ = w.Store.Put(ctx, job)
		// Ack rather than leave it for the reclaim loop: an endlessly
		// failing teardown would otherwise retry forever. The job stays
		// in "deleting" with the error in Detail; re-issuing DELETE
		// enqueues a clean retry.
		_ = w.Queue.Ack(ctx, msg)
		return
	}

	job.Status = model.StatusDeleted
	job.Detail = ""
	job.Connection = nil
	job.HTTP = nil
	job.UpdatedAt = now()
	_ = w.Store.Put(ctx, job)
	_ = w.Queue.Ack(ctx, msg)
	log.Info("deprovisioned")
}

func now() time.Time { return time.Now().UTC() }

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
