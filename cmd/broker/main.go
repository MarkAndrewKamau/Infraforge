// Command broker is the Infraforge control plane: a lightweight service
// broker that accepts provisioning requests over HTTP.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/api"
	"github.com/MarkAndrewKamau/infraforge/internal/queue"
	"github.com/MarkAndrewKamau/infraforge/internal/store"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Structured logging. (Instead of server started you get time=... level=INFO msg="broker listening" addr=:8080)
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// If env BROKER_ADDR is not set, default to :8080.
	addr := envOr("BROKER_ADDR", ":8080")

	// Connect to Redis (the shared state store + job queue). Fail fast if
	// it's unreachable — a broker that can't enqueue is useless.
	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Error("cannot reach redis", "addr", rdb.Options().Addr, "err", err)
		os.Exit(1)
	}

	q := queue.NewRedis(rdb)
	if err := q.EnsureGroup(context.Background()); err != nil {
		log.Error("cannot create consumer group", "err", err)
		os.Exit(1)
	}

	// HTTP server creation
	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(store.NewRedis(rdb), q, log).Routes(),
	}

	// Run the listener in a goroutine so main can block on the signal
	// context and drive a graceful shutdown.
	go func() {
		log.Info("broker listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
