BIN := bin
PKGS := ./...

.PHONY: help build test coverage vet fmt fmt-check validate run demo docker clean

help: ## List the available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

build: ## Build the orchestrator and example binaries into ./bin
	go build -trimpath -o $(BIN)/ ./cmd/... ./examples/...

test: ## Run the tests with the race detector
	go test -race -count=1 $(PKGS)

coverage: ## Run the tests and write coverage.html
	go test -race -count=1 -coverprofile=coverage.out $(PKGS)
	go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1

vet: ## Run go vet
	go vet $(PKGS)

fmt: ## Format all Go files
	gofmt -w .

fmt-check: ## Fail if any Go file needs formatting
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run 'make fmt'" && exit 1)

validate: fmt-check vet test ## Format check, vet and tests: run before you push

run: ## Run the orchestrator locally
	go run ./cmd/orchestrator

demo: ## Run the orchestrator and three echo services with Docker Compose
	docker compose up --build

docker: ## Build the Docker image
	docker build -t microservices-orchestrator .

clean: ## Remove build and coverage output
	rm -rf $(BIN) coverage.out coverage.html
