.PHONY: help test test-coverage test-integration bench build clean lint fmt run

# Default target
help:
	@echo "Available targets:"
	@echo "  make test              - Run all unit tests"
	@echo "  make test-coverage     - Run tests with coverage report"
	@echo "  make test-integration  - Run integration tests"
	@echo "  make bench             - Run benchmarks"
	@echo "  make build             - Build binary"
	@echo "  make clean             - Clean build artifacts"
	@echo "  make lint              - Run linters"
	@echo "  make fmt               - Format code"
	@echo "  make run               - Run with example"

# Run all unit tests
test:
	@echo "🧪 Running unit tests..."
	go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "📊 Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "✅ Coverage report generated: coverage.html"
	go tool cover -func=coverage.out | grep total

# Run integration tests
test-integration:
	@echo "🔗 Running integration tests..."
	go test -v -tags=integration ./...

# Run benchmarks
bench:
	@echo "⚡ Running benchmarks..."
	go test -bench=. -benchmem ./...

# Build binary
build:
	@echo "🔨 Building binlog-info..."
	go build -o bin/binlog-info -ldflags="-s -w" .
	@echo "✅ Binary built: bin/binlog-info"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning..."
	rm -rf bin/ coverage.out coverage.html
	go clean
	@echo "✅ Clean complete"

# Run linters
lint:
	@echo "🔍 Running linters..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

# Format code
fmt:
	@echo "✨ Formatting code..."
	go fmt ./...
	@echo "✅ Code formatted"

# Run with example (requires binlog directory)
run:
	@echo "🚀 Running example..."
	@if [ -z "$(DIR)" ]; then \
		echo "Error: Please specify binlog directory with DIR=/path/to/binlogs"; \
		exit 1; \
	fi
	@if [ -z "$(GTID)" ]; then \
		echo "Error: Please specify GTID with GTID=uuid:txn"; \
		exit 1; \
	fi
	go run . -dir=$(DIR) -gtid=$(GTID) -verbose

# Install dependencies
deps:
	@echo "📦 Installing dependencies..."
	go mod download
	go mod tidy
	@echo "✅ Dependencies installed"
