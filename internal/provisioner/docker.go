package provisioner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/MarkAndrewKamau/infraforge/internal/model"
)

// Shell provisions a job's resources by shelling out to the docker CLI.
//
// Each job produces two siblings: a Postgres database and a companion
// HTTP microservice. They share the infraforge.job=<id> label so a
// single label query can list or tear down all of a job's containers,
// no matter how many kinds it grows to have.
//
// Why shell-out and not the Docker SDK? The SDK pulls hundreds of
// indirect modules and obscures what is actually happening; every
// command this code runs is one you can paste into a terminal to
// debug. The Provisioner interface keeps the door open to a SDK or
// Kubernetes backend later.
type Shell struct {
	postgresImage string
	echoImage     string
	log           *slog.Logger
}

func NewShell(log *slog.Logger) *Shell {
	return &Shell{
		postgresImage: "postgres:16-alpine",
		echoImage:     "infraforge/echo:dev",
		log:           log,
	}
}

// Provision brings both sibling resources up and returns how to reach
// them. Order matters: Postgres first, then the HTTP service. A failure
// in either leaves the other behind, which Deprovision will clean up if
// the worker later abandons the job.
func (p *Shell) Provision(ctx context.Context, j *model.Job) (*Result, error) {
	conn, err := p.provisionPostgres(ctx, j)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	httpEP, err := p.provisionHTTP(ctx, j)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	return &Result{Connection: conn, HTTP: httpEP}, nil
}

// Deprovision removes every container labeled with this job's ID. A
// single label query catches both siblings (and any future ones we add),
// so callers do not need to know how many resources a job owns.
func (p *Shell) Deprovision(ctx context.Context, j *model.Job) error {
	out, err := exec.CommandContext(ctx, "docker", "ps", "-aq",
		"-f", "label=infraforge.job="+j.ID).Output()
	if err != nil {
		return fmt.Errorf("docker ps: %w", err)
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return nil // already gone — idempotent success
	}
	args := append([]string{"rm", "-f"}, ids...)
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("docker rm -f: %w: %s", err, strings.TrimSpace(string(out)))
	}
	p.log.Info("containers removed", "job", j.ID, "count", len(ids))
	return nil
}

func (p *Shell) provisionPostgres(ctx context.Context, j *model.Job) (*model.ConnectionInfo, error) {
	name := postgresName(j)

	// Idempotency: an at-least-once redelivery may run us a second time;
	// reuse what exists rather than failing on a duplicate name.
	if inf, ok, err := dockerInspect(ctx, name); err != nil {
		return nil, err
	} else if ok {
		p.log.Info("postgres container exists, reusing",
			"name", name, "running", inf.State.Running)
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

	creds := newCreds(j)
	args := []string{
		"run", "-d",
		"--name", name,
		"-p", "127.0.0.1::5432",
		"-e", "POSTGRES_USER=" + creds.user,
		"-e", "POSTGRES_PASSWORD=" + creds.pass,
		"-e", "POSTGRES_DB=" + creds.db,
		"--label", "infraforge=true",
		"--label", "infraforge.job=" + j.ID,
		"--label", "infraforge.service=" + j.ServiceName,
		"--label", "infraforge.kind=postgres",
		"--restart", "unless-stopped",
		p.postgresImage,
	}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("docker run postgres: %w: %s", err, strings.TrimSpace(string(out)))
	}

	inf, _, err := dockerInspect(ctx, name)
	if err != nil {
		return nil, err
	}
	conn, err := connectionFromInspect(inf)
	if err != nil {
		return nil, err
	}
	// Trust the creds we just set, not what env reflection produced.
	conn.Username, conn.Password, conn.Database = creds.user, creds.pass, creds.db

	if err := waitForPostgres(ctx, name, creds.user, creds.db, 30*time.Second); err != nil {
		return nil, fmt.Errorf("postgres not ready: %w", err)
	}
	return conn, nil
}

func (p *Shell) provisionHTTP(ctx context.Context, j *model.Job) (*model.HTTPEndpoint, error) {
	name := httpName(j)

	if inf, ok, err := dockerInspect(ctx, name); err != nil {
		return nil, err
	} else if ok {
		p.log.Info("http container exists, reusing",
			"name", name, "running", inf.State.Running)
		if !inf.State.Running {
			if err := dockerStart(ctx, name); err != nil {
				return nil, err
			}
			if inf, _, err = dockerInspect(ctx, name); err != nil {
				return nil, err
			}
		}
		host, port, err := hostPortOf(inf, "8080/tcp")
		if err != nil {
			return nil, err
		}
		if err := waitForHTTP(ctx, host, port, "/health", 15*time.Second); err != nil {
			return nil, err
		}
		return &model.HTTPEndpoint{Host: host, Port: port}, nil
	}

	args := []string{
		"run", "-d",
		"--name", name,
		"-p", "127.0.0.1::8080",
		"-e", "SERVICE_NAME=" + j.ServiceName,
		"-e", "JOB_ID=" + j.ID,
		"--label", "infraforge=true",
		"--label", "infraforge.job=" + j.ID,
		"--label", "infraforge.service=" + j.ServiceName,
		"--label", "infraforge.kind=http",
		"--restart", "unless-stopped",
		p.echoImage,
	}
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		s := strings.ToLower(string(out))
		if strings.Contains(s, "no such image") ||
			strings.Contains(s, "pull access denied") ||
			strings.Contains(s, "manifest unknown") {
			return nil, fmt.Errorf(
				"image %s not present locally; build it once with `make build-echo`: %w",
				p.echoImage, err)
		}
		return nil, fmt.Errorf("docker run echo: %w: %s", err, strings.TrimSpace(string(out)))
	}

	inf, _, err := dockerInspect(ctx, name)
	if err != nil {
		return nil, err
	}
	host, port, err := hostPortOf(inf, "8080/tcp")
	if err != nil {
		return nil, err
	}
	if err := waitForHTTP(ctx, host, port, "/health", 15*time.Second); err != nil {
		return nil, fmt.Errorf("echo not ready: %w", err)
	}
	return &model.HTTPEndpoint{Host: host, Port: port}, nil
}

func postgresName(j *model.Job) string { return "infraforge-pg-" + j.ID }
func httpName(j *model.Job) string     { return "infraforge-svc-" + j.ID }

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

// hostPortOf returns the host IP and host port mapped to the given
// internal container port (e.g. "5432/tcp", "8080/tcp").
func hostPortOf(inf *inspect, internalPort string) (string, int, error) {
	bindings, ok := inf.NetworkSettings.Ports[internalPort]
	if !ok || len(bindings) == 0 {
		return "", 0, fmt.Errorf("no host port mapping for %s", internalPort)
	}
	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil {
		return "", 0, fmt.Errorf("invalid host port %q: %w", bindings[0].HostPort, err)
	}
	host := bindings[0].HostIp
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return host, port, nil
}

func connectionFromInspect(inf *inspect) (*model.ConnectionInfo, error) {
	host, port, err := hostPortOf(inf, "5432/tcp")
	if err != nil {
		return nil, err
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

// waitForHTTP polls path with a short per-attempt timeout. The echo
// image is distroless: no shell, no busybox, no curl. We probe it from
// the worker's host using the published port instead of docker exec.
func waitForHTTP(ctx context.Context, host string, port int, path string, timeout time.Duration) error {
	dl := time.Now().Add(timeout)
	url := fmt.Sprintf("http://%s:%d%s", host, port, path)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(dl) {
			return fmt.Errorf("http readiness timed out for %s", url)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
