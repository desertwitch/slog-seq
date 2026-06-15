.PHONY: benchmark check check-slop help lint test test-coverage

benchmark: ## Runs the benchmark suite
	go test -run=^$$ -bench=. -benchmem ./...
	cd seqotel && go test -run=^$$ -bench=. -benchmem ./...

check: check-slop lint test ## Runs lint and tests

check-slop: ## Checks relevant text files for punctuation used by AI
	@grep -RInP \
		--exclude-dir=vendor \
		--include='*.go' \
		--include='*.txt' \
		--include='*.yaml' \
		--include='*.yml' \
		--include='*.md' \
		'[\x{2013}\x{2014}\x{2018}\x{2019}\x{201C}\x{201D}\x{2026}\x{00A0}]' . ; \
	rc=$$?; \
	if [ $$rc -eq 0 ]; then \
		exit 1; \
	elif [ $$rc -eq 1 ]; then \
		exit 0; \
	else \
		exit $$rc; \
	fi

help: ## Shows all build related commands of the Makefile
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: ## Runs the linter on the library code
	@golangci-lint cache clean
	@golangci-lint run
	@cd seqotel && golangci-lint run

test: ## Runs all tests with race detection
	@go test -failfast -race -covermode=atomic ./...
	@cd seqotel && go test -failfast -race -covermode=atomic ./...

test-coverage: ## Runs all coverage tests for and on the library code
	@go test -failfast -race -covermode=atomic -coverpkg=./... -coverprofile=coverage.tmp ./... && \
	grep -v "mock_" coverage.tmp > coverage.txt && \
	rm coverage.tmp
	@cd seqotel && go test -failfast -race -covermode=atomic -coverpkg=./... -coverprofile=coverage.tmp ./... && \
	grep -v "mock_" coverage.tmp > coverage-otel.txt && \
	rm coverage.tmp
