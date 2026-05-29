// Command worker is the Infraforge background provisioner: it consumes
// the Redis Stream the broker enqueues onto, and turns each job into a
// real Postgres container.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/provisioner"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
	"github.com/MarkAndrewKamau/infraforge/internal/worker"
	"github.com/MarkAndrewKamau/infraforge/internal/xdsclient"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Error("cannot reach redis", "addr", rdb.Options().Addr, "err", err)
		os.Exit(1)
	}

	q := queue.NewRedis(rdb)
	if err := q.EnsureGroup(context.Background()); err != nil {
		log.Error("cannot create consumer group", "err", err)
		os.Exit(1)
	}

	// Pick the xDS client based on whether a control plane is running.
	// XDS_ADDR unset -> noop, and the worker is indistinguishable from
	// Phase 4 (provisioning still works, just no live Envoy routing).
	var xds xdsclient.Client = xdsclient.Noop{}
	if addr := os.Getenv("XDS_ADDR"); addr != "" {
		xds = xdsclient.NewHTTP(addr)
		log.Info("xds enabled", "addr", addr)
	}

	w := &worker.Worker{
		Name:      envOr("WORKER_NAME", hostname()+"-1"),
		Store:     store.NewRedis(rdb),
		Queue:     q,
		Provision: provisioner.NewShell(log),
		XDS:       xds,
		Log:       log,
		// Reclaim and retry tunables. Zero values fall back to the
		// worker package defaults; the env vars exist mostly so crash
		// recovery can be exercised on a short clock during testing.
		ReclaimEvery:   envDur("WORKER_RECLAIM_EVERY", 0, log),
		ReclaimMinIdle: envDur("WORKER_RECLAIM_MIN_IDLE", 0, log),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	w.Run(ctx)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envDur parses a duration env var (e.g. "15s", "2m"). An unset var or an
// unparseable value yields def, so a typo degrades to the default rather
// than crashing the worker.
func envDur(k string, def time.Duration, log *slog.Logger) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Warn("invalid duration env var, using default",
			"key", k, "value", v, "default", def)
		return def
	}
	return d
}

func hostname() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "worker"
}
