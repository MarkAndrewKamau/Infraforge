# Infraforge

A miniature internal Platform-as-a-Service (PaaS), built from scratch on a
single laptop to study the architecture of large internal developer
platforms such as Atlassian's Micros. Clients POST a declarative
provisioning request to a lightweight Go service broker; an asynchronous
worker picks the work up off a Redis Stream and brings a real,
network-reachable Postgres container into existence. The result is a
small, legible system that exercises the same control-loop pattern that
larger production platforms rely on.

This repository is primarily a learning artifact. The code is deliberately
small, dependency-light, and free of speculative abstractions.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [HTTP API Reference](#http-api-reference)
- [How It Works](#how-it-works)
- [Testing](#testing)
- [Development](#development)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Implementation Status](#implementation-status)
- [Planned Work](#planned-work)
- [References](#references)

## Overview

Infraforge models the core control loop of an internal PaaS:

1. A client describes a desired resource (for example, a Postgres database
   belonging to a service called `checkout`) and submits it to a broker
   over HTTP.
2. The broker validates the request, records it in durable state, and
   places a reference to the job onto a Redis Stream. It returns
   immediately with a job identifier; provisioning happens out of band.
3. A separate worker process consumes the stream, executes the
   provisioning steps for the requested resource type, and writes the
   outcome back to the shared state store.
4. The client polls the broker for status until the job reaches a terminal
   state (`ready` or `failed`), at which point the response includes the
   connection details needed to reach the new resource.

The codebase is partitioned so each concern (HTTP surface, state, queue,
provisioning) is replaceable. Adding a new resource type, swapping the
state backend, or moving from local Docker to Kubernetes touches one
package and not the others.

## Architecture

```
                client
                  |
                  | (1) POST /v1/provision
                  v
          +-----------------+
          |     broker      |   Go HTTP service. Validates the request,
          |  (cmd/broker)   |   persists the job, enqueues a reference.
          +--------+--------+
                   |
                   | (2) SET infraforge:job:<id>
                   | (3) XADD infraforge:jobs * job_id <id>
                   v
          +-----------------+
          |      Redis      |   State store (JSON per job, 24h TTL) and
          |     (compose)   |   Stream-based job queue with consumer
          +--------+--------+   groups for at-least-once delivery.
                   ^
                   | (4) XREADGROUP / XACK
                   |
          +--------+--------+
          |     worker      |   Long-running consumer. Loads the job,
          |  (cmd/worker)   |   runs the provisioner, writes back the
          +--------+--------+   outcome (ready or failed).
                   |
                   | (5) docker run postgres:16-alpine ...
                   v
          +-----------------+
          |  Postgres       |   Per-job container, deterministically named
          | (infraforge-pg- |   infraforge-pg-<jobID>, bound to a random
          |     <jobID>)    |   host port on 127.0.0.1.
          +-----------------+
                   ^
                   | (6) GET /v1/provision/<id> returns connection info
                   |     once status reaches "ready"; client connects.
                  client
```

### Components

| Component | Path | Responsibility |
|-----------|------|----------------|
| Broker | `cmd/broker` | HTTP entrypoint. Validates requests, persists jobs, enqueues. |
| Worker | `cmd/worker` | Consumes the queue, drives the provisioner, updates state. |
| API | `internal/api` | HTTP handlers and routing for the broker. |
| Model | `internal/model` | Wire and state types shared across packages. |
| Store | `internal/store` | Job state. In-memory and Redis implementations behind one interface. |
| Queue | `internal/queue` | Redis Streams producer and consumer. |
| Provisioner | `internal/provisioner` | Resource lifecycle. Current implementation shells out to the Docker CLI. |
| Worker logic | `internal/worker` | Control loop. Testable in isolation via fakes. |
| Infrastructure | `docker-compose.yml` | Redis. |
| CI | `.github/workflows/ci.yml` | `go vet`, `go build`, `go test -race` on every push. |

## Project Structure

```
.
|-- cmd/
|   |-- broker/                  HTTP control-plane entrypoint
|   |   `-- main.go
|   `-- worker/                  Background provisioning worker entrypoint
|       `-- main.go
|-- internal/
|   |-- api/                     HTTP handlers and routing
|   |   |-- api.go
|   |   `-- api_test.go
|   |-- model/                   Shared types (Job, Status, ConnectionInfo)
|   |   `-- model.go
|   |-- store/                   Job state interface and implementations
|   |   |-- store.go             Interface + in-memory implementation
|   |   `-- redis.go             Redis-backed implementation
|   |-- queue/                   Redis Streams producer/consumer
|   |   `-- queue.go
|   |-- provisioner/             Resource lifecycle implementations
|   |   |-- provisioner.go       Interface
|   |   `-- docker.go            Docker CLI implementation
|   `-- worker/                  Worker control loop
|       |-- worker.go
|       `-- worker_test.go
|-- .github/workflows/ci.yml     Continuous integration
|-- docker-compose.yml           Redis container
|-- Makefile                     Common developer commands
|-- go.mod
`-- README.md
```

## Prerequisites

| Requirement | Tested With | Purpose |
|-------------|-------------|---------|
| Go | 1.26 | Build broker and worker |
| Docker Engine | 29.x | Run Redis and provisioned Postgres containers |
| Docker Compose plugin | 5.x | Bring up Redis from `docker-compose.yml` |
| Linux user in `docker` group | n/a | Worker shells out to `docker` without sudo |
| `curl`, `python3` | any recent | Issue requests, parse JSON in shell |

Verify your environment:

```bash
go version
docker --version
docker compose version
docker info >/dev/null    # exit 0 means daemon access works
```

## Quick Start

Open three terminals at the repository root.

**Terminal 1.** Start Redis, then the broker:

```bash
make deps-up
make broker
```

**Terminal 2.** Start the worker:

```bash
make worker
```

**Terminal 3.** Provision a Postgres and watch the lifecycle:

```bash
ID=$(curl -s -X POST localhost:8080/v1/provision \
  -d '{"service_name":"checkout","resource":"postgres"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
echo "job id: $ID"

python3 - <<PY
import json, time, urllib.request
url = f"http://localhost:8080/v1/provision/$ID"
for _ in range(60):
    r = json.loads(urllib.request.urlopen(url).read())
    print(r["status"], flush=True)
    if r["status"] in ("ready", "failed"):
        print(json.dumps(r, indent=2)); break
    time.sleep(2)
PY
```

When the job reports `ready`, the response includes a `connection`
object. Talk to the new database:

```bash
docker exec infraforge-pg-$ID psql -U <username> -d <database> \
  -c 'SELECT version();'
```

To shut down:

```bash
# Ctrl-C the broker and the worker.
make clean-pg       # remove every Postgres container Infraforge created
make deps-down      # stop Redis (volume data persists)
```

## Configuration

All runtime knobs are environment variables. Defaults are listed below.

### Broker (`cmd/broker`)

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_ADDR` | `:8080` | Address the HTTP server binds to. |
| `REDIS_ADDR` | `localhost:6379` | Redis host and port for state and queue. |

### Worker (`cmd/worker`)

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `localhost:6379` | Redis host and port. |
| `WORKER_NAME` | `<hostname>-1` | Consumer name within the Redis stream group. Each running worker needs a distinct name. |

## HTTP API Reference

All payloads are JSON. All responses set `Content-Type: application/json`.

### `GET /healthz`

Liveness probe. Returns immediately without touching Redis.

Response `200 OK`:
```json
{ "status": "ok" }
```

### `POST /v1/provision`

Submit a new provisioning request. Returns immediately; provisioning
happens asynchronously.

Request body:
```json
{
  "service_name": "checkout",
  "resource":     "postgres"
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `service_name` | string | yes | Logical name of the service that will own this resource. |
| `resource` | string | no | Resource kind. Defaults to `postgres`. Currently the only accepted value. |

Response `202 Accepted` with a `Location` header pointing to the status
endpoint:
```json
{
  "id":           "af63b1957269cb82",
  "service_name": "checkout",
  "resource":     "postgres",
  "status":       "pending",
  "created_at":   "2026-05-20T07:36:59.695114344Z",
  "updated_at":   "2026-05-20T07:36:59.695114344Z"
}
```

Error responses:

| Status | Condition | Body |
|--------|-----------|------|
| 400 | malformed JSON, missing `service_name`, or unsupported `resource` | `{"error":"..."}` |
| 500 | state store or queue failure | `{"error":"..."}` |

### `GET /v1/provision/{id}`

Fetch the current state of a job.

Response `200 OK` once the job reaches `ready`:
```json
{
  "id":           "af63b1957269cb82",
  "service_name": "checkout",
  "resource":     "postgres",
  "status":       "ready",
  "connection": {
    "host":     "127.0.0.1",
    "port":     32768,
    "username": "u_bc13ef6534202dea",
    "password": "0ec43d11875b808737910571894be7d1",
    "database": "app_checkout"
  },
  "created_at":   "2026-05-20T07:36:59.695114344Z",
  "updated_at":   "2026-05-20T07:37:09.337163547Z"
}
```

Job lifecycle:

```
pending  -->  provisioning  -->  ready
                  |
                  +-->  failed   (detail field carries the reason)
```

Error responses:

| Status | Condition |
|--------|-----------|
| 404 | No job recorded for the given identifier (or the 24h TTL has elapsed). |
| 500 | State store unreachable. |

## How It Works

A few design choices are worth understanding before reading the code.

**Asynchronous accept.** The broker never performs provisioning inline.
Pulling an image and starting a container is slow and failure-prone, so
the API returns `202 Accepted` the moment it has durably recorded the
job. Callers poll for the outcome. This is the same shape that real
internal PaaS APIs use, and it is the reason the system can keep working
when provisioning is slow.

**Persist before enqueue.** The broker writes the job to the state store
before pushing a reference onto the queue. If the store write fails, the
caller gets a 500 and no worker ever sees the job. If the enqueue fails
after the store write, the job is updated to `failed` and the caller
gets a 500. The system never returns 202 for work no worker will pick
up.

**Redis Streams with a consumer group, not LPUSH/BRPOP.** A list-based
queue deletes the entry the moment the worker reads it; a crash mid-work
loses the job. A stream with a consumer group keeps the entry in the
group's pending entries list until the worker explicitly acks it. The
pending list is the foundation for the retry and dead-letter behaviour
described under [Planned Work](#planned-work).

**Persist before ack.** The worker writes the terminal job state to the
store before issuing `XACK`. If the ack fails, redelivery will hit the
"already ready" early-return in the worker and ack the redelivered
message harmlessly.

**Deterministic resource names.** Each provisioned Postgres container is
named `infraforge-pg-<jobID>`. On message redelivery the worker inspects
this name first: if a container already exists, it is reused (its
connection details are read back from inspect output) instead of
recreating. This is what makes at-least-once delivery survivable end to
end.

**`pg_isready` for readiness.** Postgres opens its TCP listener before
the database is ready to accept queries. A raw TCP dial returns green
prematurely. The worker shells `pg_isready` into the container, which is
the probe Postgres itself ships, so the broker only ever returns
`ready` once a real client could connect.

**Replaceable seams.** `store.Store`, `queue.Queue`, and
`provisioner.Provisioner` are interfaces. The worker depends on the
interfaces, not the concrete types. This is what allows the unit tests
to run without Docker, and what will allow a Kubernetes provisioner to
drop in alongside the current Docker one.

## Testing

Two complementary layers.

### Unit tests

Fast, deterministic, no external dependencies. The API tests use an
in-memory store and a fake queue; the worker tests use an in-memory
store, a fake queue, and a fake provisioner. CI runs exactly these.

```bash
make vet            # static analysis
make test           # go test ./... -race
go build ./...      # everything compiles
```

### Manual end-to-end

Exercises the real Redis, the real broker, the real worker, and a real
Postgres container. Follow the [Quick Start](#quick-start) and then
inspect each layer:

```bash
# Job persisted in Redis:
docker compose exec redis redis-cli GET "infraforge:job:$ID"

# Stream depth and contents:
docker compose exec redis redis-cli XLEN infraforge:jobs
docker compose exec redis redis-cli XRANGE infraforge:jobs - +

# Consumer group state (should show 1 consumer, 0 pending after success):
docker compose exec redis redis-cli XINFO GROUPS infraforge:jobs

# Provisioned container details:
docker ps --filter label=infraforge=true \
  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Labels}}'

# Negative cases:
curl -i -X POST localhost:8080/v1/provision -d '{}'
curl -i -X POST localhost:8080/v1/provision \
  -d '{"service_name":"x","resource":"mysql"}'
curl -i localhost:8080/v1/provision/deadbeef
```

### Continuous integration

`.github/workflows/ci.yml` runs `go vet`, `go build`, and `go test -race`
against the Go version declared in `go.mod` on every push and pull
request.

## Development

### Make targets

| Target | Purpose |
|--------|---------|
| `make broker` | Run the broker against the current Go source. |
| `make worker` | Run the worker against the current Go source. |
| `make test` | Run the unit test suite with the race detector. |
| `make vet` | Run `go vet ./...`. |
| `make tidy` | Run `go mod tidy`. |
| `make deps-up` | Start Redis via `docker compose`. |
| `make deps-down` | Stop Redis. |
| `make clean-pg` | Remove every container the worker has provisioned (matched by `label=infraforge=true`). |

### Coding conventions

- Every external behaviour belongs behind an interface in `internal/`.
  Concrete implementations live in the same package as the interface.
- Errors are returned with enough context to identify the source
  (operation name plus, where useful, the captured stderr of a shelled
  command).
- HTTP handlers do not generate IDs or timestamps in two places; both
  are stamped once at the moment the job is constructed.
- No emojis or decorative characters in code, comments, or commit
  messages.

### Adding a new resource type

1. Add a constant to `model.ResourceType`.
2. Extend the validation switch in `internal/api/api.go`.
3. Add a `Provisioner` implementation (or extend the existing one to
   branch on resource kind).
4. Add unit tests with a fake provisioner.

### Adding a new state backend

Implement `store.Store` in a new file under `internal/store/` and wire
it up in `cmd/broker/main.go` and `cmd/worker/main.go`.

## Operations

### Logging

Both binaries log to stdout using `log/slog` in the default text format.
A typical successful provisioning produces:

```
broker:  msg="broker listening" addr=:8080
broker:  msg="provision requested" id=af63b1957269cb82 service=checkout resource=postgres
worker:  msg="worker started" name=hostname-1
worker:  msg=provisioned job=af63b1957269cb82 stream_id=... host=127.0.0.1 port=32768 db=app_checkout
```

### Inspecting state

Useful commands when investigating a job:

```bash
# State record:
docker compose exec redis redis-cli GET infraforge:job:<id>

# All pending stream entries (jobs delivered to a consumer but not acked):
docker compose exec redis redis-cli XPENDING infraforge:jobs provisioners

# Container logs (often the answer to "why did it fail"):
docker logs infraforge-pg-<id>
```

### Cleanup

```bash
make clean-pg                                           # remove all provisioned Postgres
docker compose exec redis redis-cli FLUSHALL            # clear Redis state and stream
make deps-down                                          # stop Redis
docker volume rm infraforge_redis-data                  # wipe persistent Redis volume
```

## Troubleshooting

| Symptom | Likely cause | Resolution |
|---------|--------------|------------|
| Broker exits with `bind: address already in use` | A previous broker process is still running. | `ss -ltnp \| grep ':8080 '` to find the PID, then `kill <pid>`. `go run` spawns a compiled child; killing the wrapper is not enough. |
| Worker exits with `cannot reach redis` | Redis is down or `REDIS_ADDR` is wrong. | `make deps-up`; confirm `docker compose exec redis redis-cli PING` returns `PONG`. |
| `docker: permission denied` from the worker | The user running the worker is not in the `docker` group. | `sudo usermod -aG docker $USER`, then start a new shell. |
| Status stays on `provisioning` for over 90 seconds on the first run | The worker is pulling `postgres:16-alpine`. | Wait. Subsequent provisions take 5 to 10 seconds. To pre-warm, run `docker pull postgres:16-alpine`. |
| Job goes to `failed` with `docker inspect ...` | A transient Docker daemon error or label collision. | Read the broker and worker logs; the wrapped error includes the captured stderr from the failed command. |
| Provisioning succeeds but client cannot connect | Firewall or wrong host. | The container binds to `127.0.0.1` only; connect from the same machine. |

## Implementation Status

| Capability | Status |
|------------|--------|
| Project scaffolding, build, and CI | Implemented |
| Broker HTTP API with structured responses | Implemented |
| In-memory state store for unit tests | Implemented |
| Redis state store with per-job TTL | Implemented |
| Redis Streams queue with consumer groups | Implemented |
| Worker control loop with idempotent redelivery handling | Implemented |
| Docker-based Postgres provisioner with `pg_isready` health gating | Implemented |
| Continuous integration (vet, build, race-enabled tests) | Implemented |

## Planned Work

The following items extend the system beyond pure database provisioning
and are scheduled as the next milestones. They are listed in priority
order.

### Companion HTTP microservice per provision

Every provisioning request will produce two resources rather than one:
the Postgres container described above, plus a small HTTP service that
represents the application using that database. The HTTP service exposes
`/health` and `/whoami` endpoints and binds to a random host port on
loopback.

Motivation: Postgres speaks raw TCP and is therefore an awkward subject
for a Layer 7 proxy. An HTTP companion gives the system a workload that
the routing layer described below can meaningfully dispatch to, and
mirrors the real-world shape where a deployed service has both a public
HTTP surface and a private datastore.

The change adds an `HTTPEndpoint` field to `model.Job` alongside the
existing `Connection`, extends the worker's provisioning step to bring
up the companion container, and labels both containers with the same
`infraforge.job=<id>` so cleanup remains a single command.

### Dynamic L7 routing via Envoy and an xDS control plane

A Go control plane will run alongside the broker, speaking the Envoy
aggregated discovery service (ADS) protocol over gRPC. When the worker
finishes provisioning a companion HTTP microservice, it notifies the
control plane; the control plane synthesises an updated Envoy
configuration (a new cluster, a new route) and pushes it to a local
Envoy data plane without a restart.

The end-to-end effect: a single Envoy listener becomes the routable
entry point for every service Infraforge has provisioned, and adding a
service is a config push, not a deploy. This is the closest analog to
what large platform teams operate as a "service mesh" or "edge router."

Implementation will lean on `github.com/envoyproxy/go-control-plane` to
avoid hand-rolling the xDS protocol. Envoy itself will run as a
container described in `docker-compose.yml`.

### Kubernetes provisioning target

The Docker provisioner will be joined by a Kubernetes provisioner that
implements the same `provisioner.Provisioner` interface. Resources will
be expressed as `StatefulSet` plus `Service` objects in a local `kind`
or `k3d` cluster, with credentials placed into a per-job Secret. The
broker, worker, control plane, and Envoy will be portable into the
cluster as Deployments.

This step makes the architecture identical in shape to the production
internal-PaaS pattern it is modeled on, and lets the system be exercised
with realistic primitives (namespaces, RBAC, Secrets, Services) rather
than Docker labels and host ports.

### Hardening: retries, dead-letter, and deprovisioning

Three related improvements grouped together:

- **Retries via stream reclaim.** A pending entry that has sat in the
  group's pending list past a configurable threshold is reclaimed via
  `XCLAIM` and retried up to a small bounded number of times. This is
  the operational payoff for the Streams choice made early in the
  build.
- **Dead-letter stream.** Jobs that exhaust retries are moved onto a
  second stream (`infraforge:jobs:dead`) carrying the last error,
  preserving them for inspection rather than silently dropping.
- **Deprovisioning endpoint.** `DELETE /v1/provision/{id}` will tear
  down every container labeled with the given job ID and mark the job
  `deleted`, restoring symmetry to the API.

### Observability

Lower priority but worth recording: structured logging is in place, but
the system would benefit from per-job correlation IDs propagated
through the broker, worker, and provisioner, plus Prometheus metrics
exposing queue depth, time-to-ready, and provisioning failure rates.

## References

- Atlassian Micros background: public talks and engineering blog posts
  from the Atlassian platform team.
- Redis Streams documentation: <https://redis.io/docs/latest/develop/data-types/streams/>.
- Envoy xDS protocol reference: <https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol>.
- Go modules used: `github.com/redis/go-redis/v9`.
