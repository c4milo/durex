// Package durex provides durable workflow execution backed by object storage.
//
// Durex ensures that workflows composed of steps run to completion even in the
// presence of process crashes. Step results are checkpointed to S3-compatible
// object storage. On restart, completed steps are skipped and their cached
// results are returned, providing exactly-once execution semantics.
//
// No database, no coordination service — one S3 bucket holds everything.
//
// # Quick Start
//
//	storage, _ := durex.NewS3Storage(durex.S3Config{Bucket: "my-bucket"})
//	executor := durex.New(storage, durex.Options{})
//
//	result, err := durex.RunWorkflow(ctx, executor, "order-123", func(wf *durex.Workflow) (string, error) {
//	    validated, err := durex.Step(wf, "validate", func(ctx context.Context) (bool, error) {
//	        return validateOrder(ctx, orderID), nil
//	    })
//	    if err != nil {
//	        return "", err
//	    }
//	    if !validated {
//	        return "", errors.New("invalid order")
//	    }
//
//	    chargeID, err := durex.Step(wf, "charge", func(ctx context.Context) (string, error) {
//	        return chargeCard(ctx, cardID, amount)
//	    })
//	    if err != nil {
//	        return "", err
//	    }
//
//	    return chargeID, nil
//	})
//
// # Design
//
// Step functions match func(context.Context) (T, error) — vanilla Go. Any
// existing function that takes a context and returns a value and error can be
// used directly as a step without wrapping. Business logic never imports durex.
//
// [RunWorkflow] and [Step] are package-level generic functions, not methods.
// This is driven by a Go language constraint (methods cannot have type
// parameters) but also produces a cleaner API: the user's workflow function is
// a plain func(*Workflow) (T, error).
//
// # Correctness
//
// Correctness follows the fencing model described by Martin Kleppmann
// (https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
// a lock alone is never sufficient because GC pauses, clock skew, and network
// partitions can cause two processes to believe they hold the lock simultaneously.
// The resource itself must enforce the fence.
//
// Durex implements this with two layers:
//
//  1. Claim (lease). Before executing a workflow, the executor creates a claim
//     object using [ObjectStorage.PutIfAbsent] (S3 If-None-Match). Only one
//     executor can create it. If the holder crashes, another executor can take
//     over after lease expiry by overwriting the claim with a bumped fence token.
//     This prevents concurrent execution in the common case.
//
//  2. Write-once step results (the fence). Every step result is persisted using
//     [ObjectStorage.PutIfAbsent]. If two executors race on the same step due to
//     a stale lease, only the first writer's result is accepted. The second
//     executor reads and adopts the winner's result. The storage itself rejects
//     stale writes, regardless of lock state.
//
// The distributed locking protocol is formally verified with TLA+ model checking
// (see tla/DurexLock.tla). The model explores all interleavings of N executors
// competing to run a workflow with K steps, including lease expiry, process
// crashes, retries, and restarts.
//
// # Recovery
//
// Failed steps are not persisted. Only successful step results are written as
// write-once objects. On re-execution with the same workflow ID:
//
//   - Completed steps are skipped (their cached results are loaded from S3).
//   - Failed steps are retried automatically.
//   - A completed workflow returns its cached result without re-execution.
//
// # Storage Layout
//
// All state lives under a configurable prefix (default "durex/"):
//
//	durex/workflows/{workflow_id}/
//	├── claim.json            # distributed lock / lease (overwritable)
//	├── state.json            # workflow status, result, error
//	└── steps/
//	    ├── {step_name}.json  # write-once step result
//	    └── ...
//
// The claim, state, and step results are separate objects so they can be written
// independently without read-modify-write cycles. Step results are immutable
// once written.
//
// # Observability
//
// Durex is instrumented with OpenTelemetry. Spans are created for workflows,
// claim acquisition, and each step (including separate spans for execution and
// persistence). The context passed to step functions carries the active span,
// so any outgoing calls (HTTP, gRPC, database) automatically become child spans.
//
// If no [go.opentelemetry.io/otel.TracerProvider] is configured, tracing is a
// no-op with zero overhead.
//
// Span hierarchy:
//
//	durex.workflow                        workflow execution
//	├── durex.workflow.acquire_claim      claim acquisition
//	├── durex.step                        per-step wrapper
//	│   ├── durex.step.execute            user function execution
//	│   └── durex.step.persist            PutIfAbsent write
//	└── durex.step                        next step...
//
// Key attributes: durex.workflow.id, durex.step.name, durex.step.cached,
// durex.step.fenced, durex.workflow.fence_token, durex.claim.action.
package durex
