# durex — Durable Workflow Execution on S3

## What this is
A Go library for durable workflow execution backed exclusively by S3-compatible
object storage. No databases, no coordination services — one S3 bucket holds
everything. Correctness follows Kleppmann's fencing model, formally verified
with TLA+.

## Development

### Prerequisites
- Go 1.21+ (uses `context.WithoutCancel`)
- `golangci-lint` v2
- `deadcode` (`golang.org/x/tools/cmd/deadcode`)
- AWS CLI + SSO for E2E tests

### Commands
```
make check       # lint + deadcode + file-length + unit tests
make test        # unit tests only
make test-e2e    # E2E tests against real S3 (loads .env.local)
make lint        # golangci-lint
make deadcode    # standalone deadcode analysis
```

### E2E tests
Copy `.env.local.example` to `.env.local` and fill in your AWS profile and
bucket. Then `make test-e2e`.

## Code conventions

### File length
All `.go` files must be under 500 lines. Enforced by `make check`.

### Linting
golangci-lint v2 with gocognit (max 20), no gocyclo. See `.golangci.yml`.

### Testing
- Unit tests use `MemoryStorage` (in-memory, no external deps).
- E2E tests (`//go:build e2e`) use real S3 via AWS SDK.
- Regression tests in `regression_test.go` cover specific bug fixes.

### Storage semantics
- **Write-once** (`PutIfAbsent`): step results, workflow results (`result.json`).
  First writer wins. This is the fencing mechanism.
- **Mutable** (`Put`): claims (`claim.json`), workflow status (`state.json`).
  Used for leases and operational state only.
- S3 keys use plain string concatenation, never `path.Join` (avoids normalization).

### Commit messages
Always explain **why** the change was made, not just what changed.

### Git workflow
- Always ask for approval before committing.
- Never do destructive operations without explicit approval: push, force-push,
  checkout (discards changes), branch deletion, file deletion, reset.
- Never force-push to main.
