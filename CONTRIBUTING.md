# Contributing to PulseFlow

Thanks for your interest in contributing to PulseFlow! This document outlines
how to get started, the development workflow, and coding standards.

## Getting Started

1. Fork the repository on GitHub.
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/pulseflow.git
   cd pulseflow
   ```
3. Install prerequisites:
   - Go 1.25+
   - Redis 7+
   - PostgreSQL 16+
   - `task` CLI (optional, for task runner)

## Development Setup

```bash
# Start infrastructure (Redis + Postgres)
cd deploy
docker compose up -d

# Run the server
go run ./cmd/server
```

## Running Tests

```bash
# Run all tests
go test ./... -count=1

# Run tests for a specific package
go test ./internal/adapter/redis/ -count=1 -v

# Run with race detection
go test -race ./... -count=1

# Run benchmarks
go test ./... -bench=. -benchmem
```

## Build & Verify

```bash
go mod tidy
go build ./...
go vet ./...
go mod verify
```

## Pull Request Process

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```
2. Make your changes following the coding standards below.
3. Write tests for all new code. Every new feature or bug fix must include
   appropriate test coverage.
4. Commit your changes with clear, descriptive messages:
   ```bash
   git commit -m "feat: add schema validation endpoint"
   ```
5. Push to your branch and open a pull request.
6. Ensure all CI checks pass (build, vet, tests, lint).

### Commit Message Convention

We follow a simple convention:

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation changes
- `refactor:` — code refactoring
- `test:` — test additions or fixes
- `chore:` — tooling, dependency updates, CI

## Coding Standards

### Go Style

- Follow standard Go idioms — see [Effective Go](https://go.dev/doc/effective_go).
- Run `gofmt` before committing:
  ```bash
  gofmt -w .
  ```
- Use `golangci-lint` if available:
  ```bash
  golangci-lint run
  ```
- No unused imports or variables.
- Exported types and functions must have doc comments.

### Architecture Rules

- Adhere to Clean Architecture: `entity` → `usecase` → `adapter`.
  Domain layer must have zero external dependencies.
- Every usecase must use `context.Context` as the first parameter.
- All new interfaces must be defined in `internal/entity/` (the domain layer).
- Adapters must implement domain interfaces — never the reverse.
- Error wrapping: use `fmt.Errorf("context: %w", err)` for wrapping.
- Prefer `errors.New` / `errors.Is` for sentinel errors defined in `entity/errors.go`.

### Testing

- All new code must have unit tests.
- Integration tests using `miniredis` are acceptable for Redis adapter tests.
- Test files must be named `*_test.go` and placed alongside the code they test.
- Tests must pass on Go 1.25+ with no warnings.

## Project Structure

```
pulseflow/
├── cmd/server/main.go      # Entry point
├── config.yaml             # Default config
├── internal/
│   ├── adapter/            # Infrastructure (Redis, Postgres, etc.)
│   ├── entity/             # Domain (zero deps)
│   ├── usecase/            # Application logic
│   └── config/             # Viper-based config loader
├── embed/                  # @pulseflow/embed React component
├── deploy/                 # Docker & Kubernetes
└── README.md
```

## Reporting Issues

- Use the provided issue templates.
- Include the Go version, PulseFlow version (or commit hash), and steps
  to reproduce.
- For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## Code of Conduct

By participating in this project, you agree to abide by the
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under
the [Business Source License 1.1](LICENSE).
