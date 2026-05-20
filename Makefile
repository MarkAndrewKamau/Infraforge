.PHONY: broker worker test vet tidy deps-up deps-down clean-pg

# Run the control-plane broker (Phase 1).
broker:
	go run ./cmd/broker

# Run the background provisioning worker (Phase 3).
worker:
	go run ./cmd/worker

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
