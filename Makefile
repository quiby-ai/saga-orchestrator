.PHONY: help build test clean sqlc-generate

# Default target
help:
	@echo "Available commands:"
	@echo "  build          - Build the application"
	@echo "  test           - Run tests"
	@echo "  clean          - Clean build artifacts"
	@echo "  sqlc-generate  - Generate Go code from SQL queries"
	@echo "  sqlc-validate  - Validate SQL queries without generating code"

# Build the application
build:
	go build ./...

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	go clean
	rm -rf internal/storage/sqlc/*.go

# Generate Go code from SQL queries
sqlc-generate:
	sqlc generate

# Validate SQL queries without generating code
sqlc-validate:
	sqlc validate

# Install sqlc if not present
sqlc-install:
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

# Development setup
dev-setup: sqlc-install sqlc-generate build
