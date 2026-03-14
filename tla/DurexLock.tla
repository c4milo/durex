---- MODULE DurexLock ----
\* Formal model of durex's distributed locking mechanism.
\*
\* Verifies that write-once step results (S3 PutIfAbsent) provide
\* Kleppmann's fencing guarantee: even when the lease-based lock fails
\* due to GC pauses, clock skew, or network partitions, no executor can
\* overwrite another's persisted step result.
\*
\* The model explores all interleavings of N executors competing to run
\* a workflow with K steps, including lease expiry, process crashes,
\* retries, and restarts.
\*
\* Safety properties verified:
\*   ResultConsistency      - All completing executors agree on step results
\*   StorageMatch           - Completed executors' caches match S3 storage
\*   CompletionIntegrity    - Completed executors have results for all steps
\*   WriteOnceProperty      - Step results in S3 never change once written
\*   FencePositive          - Fence token is always >= 1 when a claim exists
\*
\* Liveness property (requires FairSpec):
\*   EventualCompletion     - At least one executor eventually completes
\*
\* Deadlock freedom verified:
\*   The system never reaches a state where no executor has completed and
\*   no action is enabled. Crashed executors can restart, rejected executors
\*   can retry.

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Exec,       \* Set of executor IDs, e.g. {e1, e2, e3}
    NumSteps,   \* Number of workflow steps, e.g. 2
    MaxCrashes, \* Max crashes per executor (bounds state space), e.g. 2
    NULL        \* Distinct sentinel value

ASSUME NumSteps >= 1
ASSUME Exec # {}
ASSUME NULL \notin Exec
ASSUME MaxCrashes >= 0

Steps == 1..NumSteps

VARIABLES
    \* --- S3 Storage ---
    claim,          \* NULL | [owner: Exec, fence: Nat]
    leaseAlive,     \* Is the current claim's lease still valid?
    stepStore,      \* [Steps -> NULL | <<Exec, Nat>>]  write-once step results

    \* --- Per-Executor ---
    pc,             \* [Exec -> phase string]
    curStep,        \* [Exec -> Nat]  current step index
    computed,       \* [Exec -> NULL | <<Exec, Nat>>]  value computed for current step
    cache,          \* [Exec -> [Steps -> NULL | <<Exec, Nat>>]]  local step cache
    readFence,      \* [Exec -> Nat]  fence token read during ReadClaim
    crashes         \* [Exec -> Nat]  number of times this executor has crashed

vars == <<claim, leaseAlive, stepStore, pc, curStep, computed, cache, readFence, crashes>>

\* ================================================================
\* Initial State
\* ================================================================

Init ==
    /\ claim = NULL
    /\ leaseAlive = FALSE
    /\ stepStore = [i \in Steps |-> NULL]
    /\ pc = [e \in Exec |-> "idle"]
    /\ curStep = [e \in Exec |-> 1]
    /\ computed = [e \in Exec |-> NULL]
    /\ cache = [e \in Exec |-> [i \in Steps |-> NULL]]
    /\ readFence = [e \in Exec |-> 0]
    /\ crashes = [e \in Exec |-> 0]

\* ================================================================
\* Claim Acquisition
\* ================================================================

\* PutIfAbsent on claim.json. Atomic S3 conditional write.
TryClaim(e) ==
    /\ pc[e] = "idle"
    /\ IF claim = NULL
       THEN \* PutIfAbsent succeeds -- we own the workflow
            /\ claim' = [owner |-> e, fence |-> 1]
            /\ leaseAlive' = TRUE
            /\ pc' = [pc EXCEPT ![e] = "load_steps"]
       ELSE \* Claim exists -- need to check if expired
            /\ pc' = [pc EXCEPT ![e] = "read_claim"]
            /\ UNCHANGED <<claim, leaseAlive>>
    /\ UNCHANGED <<stepStore, curStep, computed, cache, readFence, crashes>>

\* Read existing claim. Record fence token locally for TakeOver.
\* Between TryClaim and here, the claim might have been overwritten
\* by another executor (race). We read whatever is current.
ReadClaim(e) ==
    /\ pc[e] = "read_claim"
    /\ IF claim = NULL
       THEN \* Claim disappeared (released) between TryClaim and now.
            \* Go back to idle to retry.
            /\ pc' = [pc EXCEPT ![e] = "idle"]
            /\ readFence' = readFence
       ELSE \* Read fence token and check lease.
            /\ readFence' = [readFence EXCEPT ![e] = claim.fence]
            /\ pc' = [pc EXCEPT ![e] =
                        IF leaseAlive THEN "rejected" ELSE "take_over"]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, curStep, computed, cache, crashes>>

\* CAS (compare-and-swap) on the expired claim via S3 conditional write
\* (If-Match: <etag>). The executor bumps the fence token by one and
\* overwrites the claim only if no other executor modified it since our
\* ReadClaim. If the claim changed (another executor took over or
\* released it), the CAS fails and we retry from idle.
\*
\* This guarantees strict fence token monotonicity: each successful
\* TakeOver increments the fence by exactly 1 from the current value.
\* Two concurrent TakeOver attempts cannot both succeed — the second
\* one sees a different etag and fails.
TakeOver(e) ==
    /\ pc[e] = "take_over"
    /\ IF claim # NULL /\ claim.fence = readFence[e]
       THEN \* CAS succeeds — claim unchanged since ReadClaim
            /\ claim' = [owner |-> e, fence |-> readFence[e] + 1]
            /\ leaseAlive' = TRUE
            /\ pc' = [pc EXCEPT ![e] = "load_steps"]
       ELSE \* CAS fails — claim was modified or released. Retry.
            /\ pc' = [pc EXCEPT ![e] = "idle"]
            /\ UNCHANGED <<claim, leaseAlive>>
    /\ UNCHANGED <<stepStore, curStep, computed, cache, readFence, crashes>>

\* ================================================================
\* Step Execution
\* ================================================================

\* Snapshot all existing step results from S3 into local cache.
\* This is the recovery path: previously completed steps are loaded
\* so they can be skipped.
LoadSteps(e) ==
    /\ pc[e] = "load_steps"
    /\ cache' = [cache EXCEPT ![e] = stepStore]
    /\ curStep' = [curStep EXCEPT ![e] = 1]
    /\ pc' = [pc EXCEPT ![e] = "check_cache"]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, computed, readFence, crashes>>

\* Check if current step has a cached result from a previous execution.
CheckCache(e) ==
    /\ pc[e] = "check_cache"
    /\ curStep[e] \in Steps
    /\ pc' = [pc EXCEPT ![e] =
                IF cache[e][curStep[e]] # NULL
                THEN "next_step"    \* skip (already completed)
                ELSE "exec_step"]   \* execute
    /\ UNCHANGED <<claim, leaseAlive, stepStore, curStep, computed, cache, readFence, crashes>>

\* Execute the step function. Produces a value unique to this executor,
\* modeling non-deterministic side effects (e.g., UUID generation, API calls
\* that return different results each time).
ExecStep(e) ==
    /\ pc[e] = "exec_step"
    /\ computed' = [computed EXCEPT ![e] = <<e, curStep[e]>>]
    /\ pc' = [pc EXCEPT ![e] = "persist_step"]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, curStep, cache, readFence, crashes>>

\* PutIfAbsent on step result. THE FENCING GUARANTEE:
\* S3 conditional write ensures only the first writer's result is accepted.
\* This is atomic -- S3 checks existence and writes in one operation.
PersistStep(e) ==
    /\ pc[e] = "persist_step"
    /\ LET i == curStep[e] IN
       IF stepStore[i] = NULL
       THEN \* PutIfAbsent succeeds -- we are the first writer
            /\ stepStore' = [stepStore EXCEPT ![i] = computed[e]]
            /\ cache' = [cache EXCEPT ![e][i] = computed[e]]
            /\ pc' = [pc EXCEPT ![e] = "next_step"]
       ELSE \* PutIfAbsent fails -- another executor wrote first
            /\ pc' = [pc EXCEPT ![e] = "adopt_result"]
            /\ UNCHANGED <<stepStore, cache>>
    /\ UNCHANGED <<claim, leaseAlive, curStep, computed, readFence, crashes>>

\* Models ambiguous PutIfAbsent response: the write succeeds on S3 but
\* the executor crashes before seeing the acknowledgement. On restart,
\* LoadSteps will read the persisted value from S3 and adopt it.
\* This closes the gap where Crash alone (which fires before the write
\* commits) does not model server-side success + client-side failure.
PersistStepThenCrash(e) ==
    /\ pc[e] = "persist_step"
    /\ crashes[e] < MaxCrashes
    /\ LET i == curStep[e] IN
       IF stepStore[i] = NULL
       THEN \* Write succeeds on S3, but executor crashes before seeing ACK
            /\ stepStore' = [stepStore EXCEPT ![i] = computed[e]]
            /\ pc' = [pc EXCEPT ![e] = "crashed"]
            /\ crashes' = [crashes EXCEPT ![e] = crashes[e] + 1]
            /\ UNCHANGED cache
       ELSE \* Already written -- normal conflict, but executor also crashes
            /\ pc' = [pc EXCEPT ![e] = "crashed"]
            /\ crashes' = [crashes EXCEPT ![e] = crashes[e] + 1]
            /\ UNCHANGED <<stepStore, cache>>
    /\ UNCHANGED <<claim, leaseAlive, curStep, computed, readFence>>

\* Read and adopt the winning executor's step result.
\* This is the "lose gracefully" path: our execution's side effects
\* may have occurred, but we use the first writer's result for consistency.
AdoptResult(e) ==
    /\ pc[e] = "adopt_result"
    /\ LET i == curStep[e] IN
       cache' = [cache EXCEPT ![e][i] = stepStore[i]]
    /\ pc' = [pc EXCEPT ![e] = "next_step"]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, curStep, computed, readFence, crashes>>

\* Advance to next step, or complete the workflow if all steps are done.
NextStep(e) ==
    /\ pc[e] = "next_step"
    /\ curStep' = [curStep EXCEPT ![e] = curStep[e] + 1]
    /\ pc' = [pc EXCEPT ![e] =
                IF curStep[e] = NumSteps
                THEN "done"
                ELSE "check_cache"]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, computed, cache, readFence, crashes>>

\* ================================================================
\* Failure Modes and Recovery
\* ================================================================

\* The current lease expires non-deterministically.
\* Models: GC pauses, clock skew, slow execution, network partitions.
\* The holder is NOT notified -- it continues executing with a stale lease.
LeaseExpires ==
    /\ leaseAlive = TRUE
    /\ leaseAlive' = FALSE
    /\ UNCHANGED <<claim, stepStore, pc, curStep, computed, cache, readFence, crashes>>

\* An executor's process crashes (e.g., SIGKILL, OOM).
\* The claim is NOT released -- other executors must wait for lease expiry.
\* Bounded by MaxCrashes to keep the state space finite.
Crash(e) ==
    /\ pc[e] \notin {"idle", "done", "rejected", "crashed"}
    /\ crashes[e] < MaxCrashes
    /\ pc' = [pc EXCEPT ![e] = "crashed"]
    /\ crashes' = [crashes EXCEPT ![e] = crashes[e] + 1]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, curStep, computed, cache, readFence>>

\* A rejected executor retries. Models the caller calling RunWorkflow again
\* after receiving ErrWorkflowAlreadyRunning.
Retry(e) ==
    /\ pc[e] = "rejected"
    /\ pc' = [pc EXCEPT ![e] = "idle"]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, curStep, computed, cache, readFence, crashes>>

\* A crashed executor restarts. Models process restart (e.g., Kubernetes,
\* systemd). The executor loses all in-memory state (cache, computed value)
\* and starts fresh from idle.
Restart(e) ==
    /\ pc[e] = "crashed"
    /\ pc' = [pc EXCEPT ![e] = "idle"]
    /\ curStep' = [curStep EXCEPT ![e] = 1]
    /\ computed' = [computed EXCEPT ![e] = NULL]
    /\ cache' = [cache EXCEPT ![e] = [i \in Steps |-> NULL]]
    /\ readFence' = [readFence EXCEPT ![e] = 0]
    /\ UNCHANGED <<claim, leaseAlive, stepStore, crashes>>

\* A completed executor voluntarily releases its claim.
\* Models the defer releaseClaim() in the Go code. Only releases if
\* the executor still owns the claim (owner check prevents deleting
\* another executor's claim after a takeover race).
ReleaseClaim(e) ==
    /\ pc[e] = "done"
    /\ claim # NULL
    /\ claim.owner = e
    /\ claim' = NULL
    /\ leaseAlive' = FALSE
    /\ UNCHANGED <<stepStore, pc, curStep, computed, cache, readFence, crashes>>

\* ================================================================
\* Specification
\* ================================================================

\* All executors have completed the workflow. This is a valid terminal state.
Terminated == \A e \in Exec: pc[e] = "done"

Next ==
    \/ \E e \in Exec:
        \/ TryClaim(e)
        \/ ReadClaim(e)
        \/ TakeOver(e)
        \/ LoadSteps(e)
        \/ CheckCache(e)
        \/ ExecStep(e)
        \/ PersistStep(e)
        \/ PersistStepThenCrash(e)
        \/ AdoptResult(e)
        \/ NextStep(e)
        \/ Crash(e)
        \/ Retry(e)
        \/ Restart(e)
        \/ ReleaseClaim(e)
    \/ LeaseExpires
    \/ (Terminated /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

\* Per-executor progress actions (excludes Crash and PersistStepThenCrash,
\* which are failure modes that should not be forced by fairness).
ProgressAction(e) ==
    \/ TryClaim(e) \/ ReadClaim(e) \/ TakeOver(e) \/ LoadSteps(e)
    \/ CheckCache(e) \/ ExecStep(e) \/ PersistStep(e)
    \/ AdoptResult(e) \/ NextStep(e) \/ Retry(e) \/ Restart(e)
    \/ ReleaseClaim(e)

\* Fair specification for liveness checking. Per-executor weak fairness
\* ensures each executor eventually makes progress when its actions are
\* continuously enabled. WF on LeaseExpires ensures expired leases are
\* eventually detected. Crash and PersistStepThenCrash are excluded:
\* failures may happen but are not required to.
FairSpec == Init /\ [][Next]_vars
    /\ (\A e \in Exec: WF_vars(ProgressAction(e)))
    /\ WF_vars(LeaseExpires)

\* ================================================================
\* Safety Invariants
\* ================================================================

\* All executors that complete the workflow agree on every step result.
\* This is the primary correctness property: despite concurrent execution,
\* lease races, and crashes, all survivors see the same workflow output.
ResultConsistency ==
    \A e1, e2 \in Exec:
        (pc[e1] = "done" /\ pc[e2] = "done") =>
            \A i \in Steps: cache[e1][i] = cache[e2][i]

\* Every completed executor's local cache matches what is actually in S3.
\* This ensures the executor didn't use a stale or phantom value.
StorageMatch ==
    \A e \in Exec:
        pc[e] = "done" =>
            \A i \in Steps: cache[e][i] = stepStore[i]

\* Every completed executor has a non-NULL result for every step.
\* No step was accidentally skipped or lost.
CompletionIntegrity ==
    \A e \in Exec:
        pc[e] = "done" =>
            \A i \in Steps: cache[e][i] # NULL

\* The fence token is always positive when a claim exists.
\* Combined with TakeOver reading claim.fence (not a stale snapshot),
\* this ensures the fence token is strictly monotonically increasing
\* across successive claim writes.
FencePositive ==
    claim # NULL => claim.fence >= 1

\* Step results in S3, once written, never change.
\* This is the core fencing property: PutIfAbsent makes step results immutable.
\* Checked as an action property: in every state transition, non-NULL values
\* in stepStore are preserved.
WriteOnce ==
    \A i \in Steps:
        stepStore[i] # NULL => stepStore'[i] = stepStore[i]

WriteOnceProperty == [][WriteOnce]_vars

\* The fence token never decreases. CAS-based TakeOver ensures each
\* successful takeover increments the fence by exactly 1, and concurrent
\* attempts are serialized by the conditional write.
FenceMonotonic ==
    (claim # NULL /\ claim' # NULL) => claim'.fence >= claim.fence

FenceMonotonicProperty == [][FenceMonotonic]_vars

\* ================================================================
\* Liveness Properties
\* ================================================================

\* At least one executor eventually completes the workflow.
\* Requires FairSpec (weak fairness on Next) to prevent trivial
\* counterexamples where the system simply stops taking steps.
EventualCompletion == <>(\E e \in Exec: pc[e] = "done")

====
