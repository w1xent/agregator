GOBIN := $(CURDIR)/bin
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

TEST_POSTGRES_DSN ?= postgres://md:md@localhost:5432/marketdata?sslmode=disable
TEST_REDIS_ADDR   ?= localhost:6379

.DEFAULT_GOAL := help
.PHONY: help tools proto build test test-integration lint up down clean-data logs tui loadtest

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-17s\033[0m %s\n", $$1, $$2}'

tools: ## Install protoc plugins into ./bin
	GOBIN=$(GOBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(GOBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

proto: tools ## Generate Go code from proto files (needs protoc)
	PATH="$(GOBIN):$$PATH" protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		marketdata/v1/marketdata.proto

build: ## Build all binaries into ./bin
	go build -o bin/ ./cmd/...

test: ## Run unit tests with the race detector
	go test -race ./...

test-integration: ## Run integration tests (start the stack with `make up` first)
	TEST_POSTGRES_DSN="$(TEST_POSTGRES_DSN)" TEST_REDIS_ADDR="$(TEST_REDIS_ADDR)" \
		go test -race -count=1 ./internal/storage ./internal/cache

lint: ## Run go vet and check formatting
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "Run gofmt on:"; gofmt -l .; exit 1)

up: ## Build and start the whole stack in Docker
	docker compose up -d --build

down: ## Stop the stack (data is kept)
	docker compose down

clean-data: ## Stop the stack and delete all data volumes
	docker compose down -v

logs: ## Follow logs of the Go services
	docker compose logs -f collector aggregator

tui: ## Start the terminal client
	go run ./cmd/tui

loadtest: ## Run the load test: 100 streams + 2000 synthetic books/s for 30s
	go run ./cmd/loadtest -subscribers 100 -rate 2000 -duration 30s
