# Design Decisions

This document captures the key design decisions behind durex and the tradeoffs they imply.

## 1. Object storage as the single source of truth

No database, no coordination service, no Redis. One S3 bucket holds everything. This forces a specific set of tradeoffs: you get infinite scale and zero infrastructure beyond a bucket, but you lose transactions, queries, and cheap conditional writes. Every design decision downstream flows from this constraint.

## 2. One object per step, not one object per workflow

Each step result is its own S3 key (`workflows/{id}/steps/{name}.json`) rather than appending to a single workflow journal. This means:

- A step checkpoint is a single `PutObject` — atomic, no read-modify-write.
- Recovery loads only completed steps via prefix listing.
- No contention on a single object as steps progress.

The tradeoff is the N+1 reads on recovery (1 `List` + N `Get` calls). A journal-based design (like Temporal's event history) would be one read but requires read-modify-write on every step, which is not atomic on S3.

## 3. Free functions with generics, not methods or interfaces

`RunWorkflow[T]` and `Step[T]` are package-level generic functions, not methods on `Executor` or `Workflow`. This was driven by a Go language constraint: methods cannot have type parameters. But it also produces a cleaner API — the user's workflow function is a plain `func(*Workflow) (T, error)`, and step functions are plain `func(context.Context) (T, error)`. No framework interfaces to implement, no struct embedding.

## 4. Step functions match `func(context.Context) (T, error)`

This is the most important API decision. The step function signature is vanilla Go — any existing function that takes a context and returns a value and error can be used directly as a step without wrapping. Your business logic never imports durex. The example test demonstrates this: `validateOrder`, `chargeCard`, `shipOrder` are plain functions passed directly to `durex.Step`.

## 5. Failed steps are not persisted

When a step fails, nothing is written to storage. Only successful step results are persisted (as write-once objects). This simplifies the recovery model: `loadAllStepResults` only finds completed steps, and failed steps are automatically retried on the next workflow execution. The alternative — persisting failure state — would complicate write-once semantics (a retry would need to overwrite the failed state) and adds no value since the user's workflow already surfaces the error via `if err != nil { return err }`.

## 6. Workflow ID as the idempotency key

The caller provides the workflow ID, not the library. This puts idempotency in the user's hands: use `"order-123"` and you get exactly-once semantics for that order. The alternative — library-generated UUIDs — would make recovery impossible without a separate ID mapping layer.

## 7. JSON as the serialization format

Step and workflow results are marshaled to `json.RawMessage`. This means results must be JSON-serializable, which excludes channels, functions, and unexported fields. The upside: stored state is human-readable, debuggable with `aws s3 cp ... - | jq`, and language-agnostic if you ever need cross-language workflows. The alternative was `encoding/gob` (richer type support but opaque) or protobuf (requires schema files).

## 8. Panic recovery at the workflow boundary

`RunWorkflow` catches panics and marks the workflow as failed. This prevents a panic in user code from leaving an orphaned "running" workflow that can never complete and blocks re-execution. The workflow can then be retried on the next call with the same ID.

## 9. Two-layer distributed locking (Kleppmann's fencing model)

Correctness follows the fencing model described by [Martin Kleppmann](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html): a lock alone is never sufficient because GC pauses, clock skew, and network partitions can cause two processes to believe they hold the lock simultaneously. The resource itself must enforce the fence.

Durex implements this with two layers:

**Layer 1 — Claim (lease).** Before executing a workflow, the executor creates a claim object (`workflows/{id}/claim.json`) using `PutIfAbsent` (S3 `If-None-Match`). Only one executor can create it. The claim carries a lease duration; if the holder crashes, another executor can take over after expiry by overwriting the claim with a bumped fence token. This prevents concurrent execution in the common case.

**Layer 2 — Write-once step results (the fence).** Every step result is persisted using `PutIfAbsent`. If two executors race on the same step (due to a stale lease), only the first writer's result is accepted. The second executor reads and adopts the winner's result. This is the Kleppmann fencing guarantee: the storage itself rejects stale writes, regardless of lock state.

The key insight: because S3 *is* both the lock store and the resource being protected, `PutIfAbsent` serves as a natural fencing mechanism. No monotonic token needs to be validated on write — immutability (write-once) is a stronger guarantee than fencing tokens, because there is no "overwrite with stale value" scenario at all.

**Tradeoff on lease takeover.** When two executors detect an expired lease simultaneously, both may overwrite the claim (only `Put`, not `PutIfAbsent`, since the claim must be replaceable). This is a deliberate choice: the lease is the efficiency layer, not the correctness layer. Even if both executors proceed, write-once step results prevent state corruption. The only risk is duplicate execution of side-effecting steps (e.g., charging a card twice), which must be handled at the application level via idempotency keys — the same requirement Temporal imposes on activities.

## 10. Storage interface with no batch operations

`ObjectStorage` has single-key `Put`/`Get`/`Delete`/`PutIfAbsent` — no `PutMulti` or `GetMulti`. This keeps implementations trivial (the memory backend is ~70 lines) but means the recovery path can't batch-fetch step results. Simplicity of the interface was chosen over recovery performance, betting that most workflows have a small number of steps.

## 11. Object storage layout

```
durex/
  workflows/
    {workflow_id}/
      claim.json              # distributed lock / lease
      state.json              # workflow status, result, error
      steps/
        {step_name}.json      # write-once step result
```

The claim, state, and step results are separate objects so they can be written independently without read-modify-write cycles. The claim is the only object that gets overwritten (on lease takeover and release). Step results are immutable once written.
