package provisioner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

// Shell provisions Postgres by shelling out to the `docker` CLI.
//
// Why shell-out, not the Docker SDK? Two reasons. (1) The SDK pulls in
// hundreds of indirect modules; for learning the control loop, that
// noise distracts from the lesson. (2) Every command this code runs is
// one you can paste into a terminal to debug. The Provisioner interface
// keeps the door open to swap in the SDK later without touching the
// worker.
type Shell struct {
	image string
	log   *slog.Logger
}

func NewShell(log *slog.Logger) *Shell {
	return &Shell{image: "postgres:16-alpine", log: log}
}

func (p *Shell) Provision(ctx context.Context, j *model.Job) (*model.ConnectionInfo, error) {
	name := containerName(j)

	// --- idempotency ---
	// Phase 2's queue is at-least-once: the same Redis message may be
	// redelivered (e.g. a worker crashed before XACK). If a container
	// with this job's deterministic name already exists, treat that as
	// the canonical resource and just read its connection info back.
	if inf, ok, err := dockerInspect(ctx, name); err != nil {
		return nil, err
	} else if ok {
		p.log.Info("container exists, reusing", "name", name, "running", inf.State.Running)
		if !inf.State.Running {
			if err := dockerStart(ctx, name); err != nil {
				return nil, err
			}
			if inf, _, err = dockerInspect(ctx, name); err != nil {
				return nil, err
			}
		}
		conn, err := connectionFromInspect(inf)
		if err != nil {
			return nil, err
		}
		if err := waitForPostgres(ctx, name, conn.Username, conn.Database, 30*time.Second); err != nil {
			return nil, err
		}
		return conn, nil
	}

	// --- create ---
	creds := newCreds(j)
	args := []string{
		"run", "-d",
		"--name", name,
		"-p", "127.0.0.1::5432", // host picks a random port on loopback
		"-e", "POSTGRES_USER=" + creds.user,
		"-e", "POSTGRES_PASSWORD=" + creds.pass,
		"-e", "POSTGRES_DB=" + creds.db,
		"--label", "infraforge=true",
		"--label", "infraforge.job=" + j.ID,
		"--label", "infraforge.service=" + j.ServiceName,
		"--restart", "unless-stopped",
		p.image,
	}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("docker run: %w: %s", err, strings.TrimSpace(string(out)))
	}

	inf, _, err := dockerInspect(ctx, name)
	if err != nil {
		return nil, err
	}
	conn, err := connectionFromInspect(inf)
	if err != nil {
		return nil, err
	}
	// Trust the creds we just set, not what the inspect reflected (they
	// should match, but env reflection is best avoided for secrets).
	conn.Username, conn.Password, conn.Database = creds.user, creds.pass, creds.db

	if err := waitForPostgres(ctx, name, creds.user, creds.db, 30*time.Second); err != nil {
		return nil, fmt.Errorf("postgres not ready: %w", err)
	}
	return conn, nil
}

// Deprovision removes the container backing this job. It is idempotent:
// `docker rm -f` on a container that is already gone is reported by
// Docker as "no such container", which we treat as success so a
// redelivered deprovision message does not fail.
func (p *Shell) Deprovision(ctx context.Context, j *model.Job) error {
	name := containerName(j)
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput()
	if err != nil {
		if strings.Contains(strings.ToLower(string(out)), "no such container") {
			return nil
		}
		return fmt.Errorf("docker rm -f %s: %w: %s", name, err, strings.TrimSpace(string(out)))
	}
	p.log.Info("container removed", "name", name)
	return nil
}

func containerName(j *model.Job) string { return "infraforge-pg-" + j.ID }

type creds struct{ user, pass, db string }

func newCreds(j *model.Job) creds {
	return creds{
		user: "u_" + randHex(8),
		pass: randHex(16),
		db:   "app_" + sanitize(j.ServiceName),
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// sanitize trims a service name down to chars Postgres accepts in
// identifiers: lowercase ASCII, digits, underscore, max 32 chars.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r == '-' || r == ' ':
			b.WriteByte('_')
		}
		if b.Len() >= 32 {
			break
		}
	}
	if b.Len() == 0 {
		return "app"
	}
	return b.String()
}

// inspect mirrors the slice of `docker inspect <name>` we actually use.
type inspect struct {
	State struct{ Running bool }
	Config struct{ Env []string }
	NetworkSettings struct {
		Ports map[string][]struct{ HostIp, HostPort string }
	}
}

func dockerInspect(ctx context.Context, name string) (*inspect, bool, error) {
	cmd := exec.CommandContext(ctx, "docker", "inspect", name)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		var stderr string
		if errors.As(err, &exitErr) {
			stderr = string(exitErr.Stderr)
			// Match case-insensitively: Docker's wording has drifted
			// across versions ("No such object" -> "no such object").
			if strings.Contains(strings.ToLower(stderr), "no such") {
				return nil, false, nil
			}
		}
		return nil, false, fmt.Errorf("docker inspect %s: %w: %s", name, err, strings.TrimSpace(stderr))
	}
	var arr []inspect
	if err := json.Unmarshal(out, &arr); err != nil {
		return nil, false, fmt.Errorf("decode inspect: %w", err)
	}
	if len(arr) == 0 {
		return nil, false, nil
	}
	return &arr[0], true, nil
}

func dockerStart(ctx context.Context, name string) error {
	if out, err := exec.CommandContext(ctx, "docker", "start", name).CombinedOutput(); err != nil {
		return fmt.Errorf("docker start: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func connectionFromInspect(inf *inspect) (*model.ConnectionInfo, error) {
	bindings, ok := inf.NetworkSettings.Ports["5432/tcp"]
	if !ok || len(bindings) == 0 {
		return nil, fmt.Errorf("no host port mapping for 5432/tcp")
	}
	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return nil, fmt.Errorf("invalid host port %q: %w", bindings[0].HostPort, err)
	}
	host := bindings[0].HostIp
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}

	// Pull creds back from POSTGRES_* env so a reused container hands the
	// caller the same credentials it actually has.
	var user, pass, db string
	for _, e := range inf.Config.Env {
		switch {
		case strings.HasPrefix(e, "POSTGRES_USER="):
			user = strings.TrimPrefix(e, "POSTGRES_USER=")
		case strings.HasPrefix(e, "POSTGRES_PASSWORD="):
			pass = strings.TrimPrefix(e, "POSTGRES_PASSWORD=")
		case strings.HasPrefix(e, "POSTGRES_DB="):
			db = strings.TrimPrefix(e, "POSTGRES_DB=")
		}
	}
	return &model.ConnectionInfo{
		Host: host, Port: port,
		Username: user, Password: pass, Database: db,
	}, nil
}

// waitForPostgres polls pg_isready inside the container. pg_isready is
// Postgres's own readiness probe: it understands the difference between
// "TCP listener open" and "ready to accept queries", which a raw TCP
// dial does not.
func waitForPostgres(ctx context.Context, name, user, db string, timeout time.Duration) error {
	dl := time.Now().Add(timeout)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(dl) {
			return fmt.Errorf("pg_isready timed out for %s", name)
		}
		cmd := exec.CommandContext(ctx, "docker", "exec", name,
			"pg_isready", "-U", user, "-d", db, "-h", "127.0.0.1", "-q")
		if err := cmd.Run(); err == nil {
			return nil
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
