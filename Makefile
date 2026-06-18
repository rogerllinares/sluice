# Sluice — task runner.
# Commands are the intended surface; some are placeholders until F0–F8 land.

.DEFAULT_GOAL := build
.PHONY: build test lint run compose-up compose-down loadtest

build: ## Compile all packages
	go build ./...

test: ## Run the full test suite with the race detector
	go test -race ./...

lint: ## Static analysis (vet + golangci-lint)
	go vet ./...
	golangci-lint run

run: ## Run the queue + worker pool locally
	go run ./cmd/sluice

compose-up: ## Start the stack (Postgres, app)
	docker compose up -d

compose-down: ## Stop the stack
	docker compose down

loadtest: ## Burst producers above pool capacity to prove backpressure/shedding
	@echo "TODO(F8): run the load generator that visibly triggers 429s / depth pinning"
