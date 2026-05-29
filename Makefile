.PHONY: broker worker xds test vet tidy deps-up deps-down clean-pg build-echo force-build-echo

# Run the control-plane broker (Phase 1).
broker:
	go run ./cmd/broker

# Run the background provisioning worker. Depends on build-echo so the
# echo image is present before the first provision request lands.
worker: build-echo
	go run ./cmd/worker

# Run the xDS control plane: gRPC ADS on :18000 (for Envoy) plus HTTP
# admin on :19000 (for the worker to register endpoints).
xds:
	go run ./cmd/xds

# Build the companion HTTP microservice image. The check skips the build
# when the image already exists locally; use force-build-echo to rebuild
# unconditionally after editing cmd/echo or Dockerfile.echo.
build-echo:
	@docker image inspect infraforge/echo:dev >/dev/null 2>&1 || \
		$(MAKE) force-build-echo

force-build-echo:
	docker build -t infraforge/echo:dev -f Dockerfile.echo .

test:
	go test ./... -race

vet:
	go vet ./...

tidy:
	go mod tidy

# Infrastructure dependencies (Redis). Used from Phase 2 onward.
deps-up:
	docker compose up -d

deps-down:
	docker compose down

# Tear down every Postgres container the worker provisioned. Handy
# between e2e runs so you start clean.
clean-pg:
	@docker ps -aq -f label=infraforge=true | xargs -r docker rm -f
