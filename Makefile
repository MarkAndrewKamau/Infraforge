.PHONY: broker test vet tidy deps-up deps-down

# Run the control-plane broker (Phase 1).
broker:
	go run ./cmd/broker

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# Infrastructure dependencies (Redis). Used from Phase 2 onward.
deps-up:
	docker compose up -d

deps-down:
	docker compose down
