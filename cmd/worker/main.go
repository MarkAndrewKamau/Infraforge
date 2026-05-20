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

	w := &worker.Worker{
		Name:      envOr("WORKER_NAME", hostname()+"-1"),
		Store:     store.NewRedis(rdb),
		Queue:     q,
		Provision: provisioner.NewShell(log),
		Log:       log,
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

func hostname() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "worker"
}
