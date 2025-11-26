.PHONY: test integration-test build clean

# Run unit tests
test:
	go test -v -short

# Run integration tests (requires Docker)
integration-test:
	go test -v -tags=integration -timeout=120s

# Run all tests
test-all: test integration-test

# Build the binary
build:
	go build -o rs-http-facade

# Clean build artifacts
clean:
	rm -f rs-http-facade
	go clean

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...

# Run linters and tests
check: fmt vet test

# Install dependencies
deps:
	go mod download
	go mod tidy
