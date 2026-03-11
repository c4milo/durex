package durex

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// C1: workflow result uses write-once — first writer wins.
func TestRunWorkflow_WriteOnceResult(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	// First execution completes and writes result.json.
	r1, err := RunWorkflow(ctx, executor, "wf-wo", func(wf *Workflow) (string, error) {
		return "first", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if r1 != "first" {
		t.Fatalf("got %q, want %q", r1, "first")
	}

	// Verify result.json exists as a separate key from state.json.
	resultKey := executor.workflowResultKey("wf-wo")
	if _, err := storage.Get(ctx, resultKey); err != nil {
		t.Fatalf("result.json should exist: %v", err)
	}

	// A second executor trying to complete with a different result should
	// still get the first writer's result (cached path).
	executor2 := New(storage, Options{})
	r2, err := RunWorkflow(ctx, executor2, "wf-wo", func(wf *Workflow) (string, error) {
		return "second", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if r2 != "first" {
		t.Fatalf("got %q, want %q (first writer should win)", r2, "first")
	}
}

// C2: panic recovery works even with cancelled context.
func TestRunWorkflow_PanicWithCancelledCtx(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx, cancel := context.WithCancel(context.Background())

	_, err := RunWorkflow(ctx, executor, "wf-panic-cancel", func(wf *Workflow) (string, error) {
		cancel() // cancel before panic
		panic("boom after cancel")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}

	// State should be saved despite cancelled context (uses cleanupCtx).
	state, err := executor.loadWorkflowState(context.Background(), "wf-panic-cancel")
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.Status != WorkflowFailed {
		t.Fatalf("got status %v, want WorkflowFailed", state)
	}
}

// H1: releaseClaim does not delete another executor's claim.
func TestReleaseClaim_DoesNotDeleteOtherOwner(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	executorA := New(storage, Options{LeaseDuration: 1}) // expires immediately
	if _, err := executorA.acquireClaim(ctx, "wf-release"); err != nil {
		t.Fatal(err)
	}

	// B takes over the expired claim.
	executorB := New(storage, Options{})
	if _, err := executorB.acquireClaim(ctx, "wf-release"); err != nil {
		t.Fatal(err)
	}

	// A tries to release — should NOT delete B's claim.
	executorA.releaseClaim(ctx, "wf-release")

	// B's claim should still exist.
	data, err := storage.Get(ctx, executorB.claimKey("wf-release"))
	if err != nil {
		t.Fatalf("B's claim was deleted by A's release: %v", err)
	}
	var claim WorkflowClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		t.Fatal(err)
	}
	if claim.Owner != executorB.ownerID {
		t.Fatalf("claim owner = %q, want %q", claim.Owner, executorB.ownerID)
	}
}

// H3: lease renewal keeps the claim alive.
func TestRenewClaim(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{LeaseDuration: time.Hour})
	ctx := context.Background()

	if _, err := executor.acquireClaim(ctx, "wf-renew"); err != nil {
		t.Fatal(err)
	}

	if !executor.renewClaim(ctx, "wf-renew") {
		t.Fatal("renewClaim should succeed for the owner")
	}

	// After another executor takes over, renewal should fail.
	other := New(storage, Options{LeaseDuration: time.Hour})
	claim := &WorkflowClaim{
		Owner:         other.ownerID,
		FenceToken:    2,
		ClaimedAt:     time.Now().UTC(),
		LeaseDuration: time.Hour,
	}
	data, _ := json.Marshal(claim)
	if err := storage.Put(ctx, executor.claimKey("wf-renew"), data); err != nil {
		t.Fatal(err)
	}

	if executor.renewClaim(ctx, "wf-renew") {
		t.Fatal("renewClaim should fail when another executor owns the claim")
	}
}

// S5: validateID rejects control characters, null bytes, and overly long IDs.
func TestValidateID_Allowlist(t *testing.T) {
	executor := New(NewMemoryStorage(), Options{})
	ctx := context.Background()

	// Valid IDs.
	for _, id := range []string{"order-123", "step_a", "my.workflow", "ABC"} {
		_, err := RunWorkflow(ctx, executor, id, func(wf *Workflow) (string, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("valid ID %q rejected: %v", id, err)
		}
	}

	// Invalid IDs: control chars, slashes, special chars.
	for _, id := range []string{
		"has/slash", "back\\slash", "null\x00byte", "newline\n",
		"tab\there", "space here", "emoji😀", "colon:bad",
	} {
		_, err := RunWorkflow(ctx, executor, id, func(wf *Workflow) (string, error) {
			return "ok", nil
		})
		if !errors.Is(err, ErrInvalidID) {
			t.Fatalf("ID %q should be rejected, got: %v", id, err)
		}
	}
}

// S5: configurable MaxIDLength.
func TestValidateID_MaxLength(t *testing.T) {
	executor := New(NewMemoryStorage(), Options{MaxIDLength: 10})
	ctx := context.Background()

	// Exactly at limit — should work.
	_, err := RunWorkflow(ctx, executor, "1234567890", func(wf *Workflow) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("ID at max length rejected: %v", err)
	}

	// Over limit — should fail.
	_, err = RunWorkflow(ctx, executor, "12345678901", func(wf *Workflow) (string, error) {
		return "ok", nil
	})
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("ID over max length should be rejected, got: %v", err)
	}
}

// S3: MaxSteps limits step loading during recovery.
func TestMaxSteps_Enforced(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{MaxSteps: 2})
	ctx := context.Background()

	// Pre-write 3 step results to simulate a workflow with too many steps.
	for _, name := range []string{"a", "b", "c"} {
		key := executor.stepKey("wf-maxsteps", name)
		data := mustMarshalStepState(t, name, "result-"+name)
		if err := storage.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}

	// Running the workflow should fail because 3 > MaxSteps(2).
	_, err := RunWorkflow(ctx, executor, "wf-maxsteps", func(wf *Workflow) (string, error) {
		return "ok", nil
	})
	if err == nil {
		t.Fatal("expected error from MaxSteps limit")
	}
	if !strings.Contains(err.Error(), "exceeding limit") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// S6: completion persists even with cancelled context.
func TestRunWorkflow_CompletionWithCancelledCtx(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx, cancel := context.WithCancel(context.Background())

	result, err := RunWorkflow(ctx, executor, "wf-cancel-complete", func(wf *Workflow) (string, error) {
		cancel() // cancel during workflow execution
		return "completed-value", nil
	})
	if err != nil {
		t.Fatalf("workflow should succeed despite cancelled ctx: %v", err)
	}
	if result != "completed-value" {
		t.Fatalf("got %q, want %q", result, "completed-value")
	}

	// Result should be persisted (via cleanupCtx).
	state, err := executor.loadWorkflowState(context.Background(), "wf-cancel-complete")
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.Status != WorkflowCompleted {
		t.Fatalf("got status %v, want WorkflowCompleted", state)
	}
}

// H5: key construction uses literal concatenation, not path.Join normalization.
func TestKeyConstruction_NoNormalization(t *testing.T) {
	executor := New(NewMemoryStorage(), Options{Prefix: "my-prefix/"})

	got := executor.workflowKey("wf-1")
	want := "my-prefix/workflows/wf-1/state.json"
	if got != want {
		t.Fatalf("workflowKey = %q, want %q", got, want)
	}

	got = executor.workflowResultKey("wf-1")
	want = "my-prefix/workflows/wf-1/result.json"
	if got != want {
		t.Fatalf("workflowResultKey = %q, want %q", got, want)
	}

	got = executor.stepKey("wf-1", "charge")
	want = "my-prefix/workflows/wf-1/steps/charge.json"
	if got != want {
		t.Fatalf("stepKey = %q, want %q", got, want)
	}

	got = executor.stepsPrefix("wf-1")
	want = "my-prefix/workflows/wf-1/steps/"
	if got != want {
		t.Fatalf("stepsPrefix = %q, want %q", got, want)
	}
}
