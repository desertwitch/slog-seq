.PHONY: benchmark check help lint test test-coverage

benchmark: ## Runs the benchmark suite
	go test -bench=. -benchmem .

check: lint test ## Runs lint and tests

help: ## Shows all build related commands of the Makefile
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: ## Runs the linter on the library code
	@golangci-lint cache clean
	@golangci-lint run

test: ## Runs all tests with race detection
	@go test -failfast -race -covermode=atomic ./...

test-coverage: ## Runs all coverage tests for and on the library code
	@go test -failfast -race -covermode=atomic -coverpkg=./... -coverprofile=coverage.tmp ./... && \
	grep -v "mock_" coverage.tmp > coverage.txt && \
	rm coverage.tmp
