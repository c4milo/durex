# durex

Durable workflow execution backed by S3. No database, no coordination service
-- one S3 bucket holds everything.

Workflows composed of steps survive process crashes. Step results are
checkpointed to S3-compatible object storage. On restart, completed steps are
skipped and their cached results are returned, providing exactly-once execution
semantics.

Correctness follows the fencing model described by
[Martin Kleppmann](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html),
formally verified with TLA+ model checking (see `tla/DurexLock.tla`).

## Quick start

```go
storage := durex.NewS3Storage(durex.S3Config{
    Bucket: "my-bucket",
    Config: awsCfg,
})
executor := durex.New(storage, durex.Options{})

result, err := durex.RunWorkflow(ctx, executor, "order-123", func(wf *durex.Workflow) (string, error) {
    valid, err := durex.Step(wf, "validate", func(ctx context.Context) (bool, error) {
        return validateOrder(ctx, order)
    })
    if err != nil {
        return "", err
    }
    if !valid {
        return "", errors.New("invalid order")
    }

    chargeID, err := durex.Step(wf, "charge", func(ctx context.Context) (string, error) {
        return chargeCard(ctx, order.Payment.CardToken, order.Amount)
    })
    if err != nil {
        return "", err
    }

    return chargeID, nil
})
```

Step functions are `func(context.Context) (T, error)` -- vanilla Go. Business
logic never imports durex.

See [`example_test.go`](example_test.go) for a complete working example.

## How it works

### Write-once fencing

Every step result and workflow result is persisted using S3 conditional writes
(`If-None-Match: *`). The first writer wins. If two executors race on the same
step due to a stale lease, only the first writer's result is accepted. The
second executor reads and adopts the winner's result:

```
Executor A                     S3                      Executor B
    |                           |                            |
    |-- execute step ---------> |                            |
    |   result: "ch_A"          |                            |
    |                           | <---- execute step --------|
    |                           |       result: "ch_B"       |
    |-- PutIfAbsent ----------> |                            |
    |   200 OK (first writer)   |                            |
    |                           | <---- PutIfAbsent ---------|
    |                           |       409 Conflict         |
    |                           |                            |
    |                           | <---- Get (adopt winner) --|
    |                           |       returns "ch_A"       |
```

The storage itself enforces the fence, not the lock. This is the key insight
from Kleppmann: a lock alone is never sufficient.

### Recovery

Failed steps are not persisted. On re-execution with the same workflow ID:

- Completed steps are skipped (cached results loaded from S3)
- Failed steps are retried automatically
- A completed workflow returns its cached result without re-execution

### Storage layout

```
durex/workflows/{workflow_id}/
├── claim.json            # distributed lock / lease (mutable)
├── state.json            # workflow status: running/failed (mutable)
├── result.json           # completed workflow result (write-once)
└── steps/
    ├── {step_name}.json  # write-once step result
    └── ...
```

## Lifecycle hooks

Hooks let you react to workflow and step events without coupling durex to any
specific backend. All hooks are optional and best-effort -- a hook failure does
not affect the workflow's durable state. Hooks fire after the durable write
succeeds, so S3 remains the source of truth regardless of hook outcome.

```go
executor := durex.New(storage, durex.Options{
    Hooks: durex.Hooks{
        OnWorkflowStart:    func(ctx context.Context, id string) { ... },
        OnWorkflowComplete: func(ctx context.Context, id string, result json.RawMessage) { ... },
        OnWorkflowFailed:   func(ctx context.Context, id string, err error) { ... },
        OnStepComplete:     func(ctx context.Context, workflowID, stepName string, result json.RawMessage) { ... },
    },
})
```

`OnStepComplete` only fires for freshly persisted steps, not cached ones from
recovery, so indexes see each step exactly once per execution.

### What you can build with hooks

**Indexing and querying** -- Write to DynamoDB or PostgreSQL on
`OnWorkflowComplete` / `OnWorkflowFailed` to query workflows by status, time
range, or customer ID without scanning S3.

**Alerting and notifications** -- `OnWorkflowFailed` to PagerDuty, Slack, or
email. `OnWorkflowComplete` to notify downstream services.

**Metrics** -- Emit Prometheus counters on each hook. Track workflow duration by
comparing `OnWorkflowStart` and `OnWorkflowComplete` timestamps.

**Event-driven architectures** -- Publish to SNS, SQS, or Kafka from
`OnWorkflowComplete` so other services react to completed orders, payments, or
shipments. `OnStepComplete` enables fine-grained step-level choreography.

**Audit logging** -- Write immutable records to a separate S3 bucket or
append-only log for compliance ("prove that step X happened at time T").

**Progress tracking** -- `OnStepComplete` to update a progress bar or push
real-time status via WebSocket ("3/5 steps done").

**Compensation and sagas** -- `OnWorkflowFailed` to trigger compensating
actions (refund the charge, cancel the shipment).

## Observability

Durex is instrumented with OpenTelemetry. Spans are created for workflows,
claim acquisition, and each step. The context passed to step functions carries
the active span, so any outgoing calls (HTTP, gRPC, database) automatically
become child spans.

```
durex.workflow
├── durex.workflow.acquire_claim
├── durex.step
│   ├── durex.step.execute       ← your function runs here
│   └── durex.step.persist       ← PutIfAbsent write
└── durex.step
    └── ...
```

If no `TracerProvider` is configured, tracing is a no-op with zero overhead.

## Configuration

```go
durex.Options{
    Prefix:        "durex/",          // S3 key prefix (default: "durex/")
    LeaseDuration: 5 * time.Minute,   // claim lease duration (default: 5m)
    StepTimeout:   30 * time.Second,  // per-step timeout (default: none)
    MaxSteps:      1000,              // max steps per workflow (default: 1000)
    MaxIDLength:   256,               // max workflow/step ID length (default: 256)
    Hooks:         durex.Hooks{...},  // lifecycle callbacks (default: none)
}
```

S3 storage also accepts limits:

```go
durex.S3Config{
    Bucket:        "my-bucket",
    Config:        awsCfg,
    UsePathStyle:  false,             // true for MinIO/LocalStack
    MaxObjectSize: 10 << 20,          // max S3 object read size (default: 10MB)
}
```

## Formal verification

The distributed locking protocol is specified in TLA+ (`tla/DurexLock.tla`)
and verified with the TLC model checker across 2.9 billion states (714 million
distinct) with no violations. The model explores all interleavings of N
executors competing to run a workflow with K steps, including lease expiry,
process crashes, retries, and restarts.

## Development

```
make check       # lint + deadcode + file-length + unit tests
make test        # unit tests only
make test-e2e    # E2E tests against real S3 (loads .env.local)
make lint        # golangci-lint
make deadcode    # standalone deadcode analysis
```

For E2E tests, copy `.env.local.example` to `.env.local` and fill in your AWS
profile and bucket.

## License

See [LICENSE](LICENSE).
