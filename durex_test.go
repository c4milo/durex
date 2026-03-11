package durex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunWorkflow_BasicExecution(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	result, err := RunWorkflow(ctx, executor, "wf-1", func(wf *Workflow) (string, error) {
		a, err := Step(wf, "step-a", func(ctx context.Context) (int, error) {
			return 10, nil
		})
		if err != nil {
			return "", err
		}

		b, err := Step(wf, "step-b", func(ctx context.Context) (int, error) {
			return a * 2, nil
		})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("result:%d", b), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "result:20" {
		t.Fatalf("got %q, want %q", result, "result:20")
	}
}

func TestRunWorkflow_CachedResult(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	var callCount atomic.Int32

	run := func() (string, error) {
		return RunWorkflow(ctx, executor, "wf-cached", func(wf *Workflow) (string, error) {
			callCount.Add(1)
			return "done", nil
		})
	}

	// First run.
	result, err := run()
	if err != nil {
		t.Fatal(err)
	}
	if result != "done" {
		t.Fatalf("got %q, want %q", result, "done")
	}

	// Second run should return cached result without calling fn.
	result, err = run()
	if err != nil {
		t.Fatal(err)
	}
	if result != "done" {
		t.Fatalf("got %q, want %q", result, "done")
	}
	if callCount.Load() != 1 {
		t.Fatalf("workflow function called %d times, want 1", callCount.Load())
	}
}

func TestRunWorkflow_Recovery(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	var stepACount, stepBCount atomic.Int32

	shouldFailB := true

	run := func() (string, error) {
		return RunWorkflow(ctx, executor, "wf-recover", func(wf *Workflow) (string, error) {
			_, err := Step(wf, "step-a", func(ctx context.Context) (string, error) {
				stepACount.Add(1)
				return "a-done", nil
			})
			if err != nil {
				return "", err
			}

			result, err := Step(wf, "step-b", func(ctx context.Context) (string, error) {
				stepBCount.Add(1)
				if shouldFailB {
					return "", errors.New("transient error")
				}
				return "b-done", nil
			})
			if err != nil {
				return "", err
			}

			return result, nil
		})
	}

	// First run: step-a succeeds, step-b fails.
	_, err := run()
	if err == nil {
		t.Fatal("expected error")
	}
	if stepACount.Load() != 1 {
		t.Fatalf("step-a executed %d times, want 1", stepACount.Load())
	}
	if stepBCount.Load() != 1 {
		t.Fatalf("step-b executed %d times, want 1", stepBCount.Load())
	}

	// Second run: step-a should be skipped (cached), step-b retries and succeeds.
	shouldFailB = false
	result, err := run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "b-done" {
		t.Fatalf("got %q, want %q", result, "b-done")
	}
	// step-a should NOT have been called again.
	if stepACount.Load() != 1 {
		t.Fatalf("step-a executed %d times, want 1 (should be cached)", stepACount.Load())
	}
	// step-b should have been called again.
	if stepBCount.Load() != 2 {
		t.Fatalf("step-b executed %d times, want 2", stepBCount.Load())
	}
}

func TestRunWorkflow_PanicRecovery(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	_, err := RunWorkflow(ctx, executor, "wf-panic", func(wf *Workflow) (string, error) {
		panic("something went wrong")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}

	// Workflow should be marked as failed.
	state, err := executor.loadWorkflowState(ctx, "wf-panic")
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != WorkflowFailed {
		t.Fatalf("got status %q, want %q", state.Status, WorkflowFailed)
	}
}

func TestRunWorkflow_InvalidID(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	_, err := RunWorkflow(ctx, executor, "", func(wf *Workflow) (string, error) {
		return "", nil
	})
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("got %v, want ErrInvalidID", err)
	}

	_, err = RunWorkflow(ctx, executor, "has/slash", func(wf *Workflow) (string, error) {
		return "", nil
	})
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("got %v, want ErrInvalidID", err)
	}
}

func TestStep_InvalidName(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	_, err := RunWorkflow(ctx, executor, "wf-invalid-step", func(wf *Workflow) (string, error) {
		_, err := Step(wf, "", func(ctx context.Context) (string, error) {
			return "x", nil
		})
		return "", err
	})
	if !errors.Is(err, ErrInvalidID) {
		t.Fatalf("got %v, want ErrInvalidID", err)
	}
}

func TestRunWorkflow_ContextCancellation(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx, cancel := context.WithCancel(context.Background())

	_, err := RunWorkflow(ctx, executor, "wf-cancel", func(wf *Workflow) (string, error) {
		_, err := Step(wf, "step-1", func(ctx context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil {
			return "", err
		}

		// Cancel context before next step.
		cancel()

		_, err = Step(wf, "step-2", func(ctx context.Context) (string, error) {
			return "should not run", nil
		})
		return "", err
	})
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestRunWorkflow_ClaimPreventsConurrent(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	// Executor A claims the workflow.
	executorA := New(storage, Options{})
	fenceToken, err := executorA.acquireClaim(ctx, "wf-locked")
	if err != nil {
		t.Fatal(err)
	}
	if fenceToken != 1 {
		t.Fatalf("got fence token %d, want 1", fenceToken)
	}

	// Executor B should be rejected.
	executorB := New(storage, Options{})
	_, err = executorB.acquireClaim(ctx, "wf-locked")
	if !errors.Is(err, ErrWorkflowAlreadyRunning) {
		t.Fatalf("got %v, want ErrWorkflowAlreadyRunning", err)
	}

	// Release and re-acquire should work.
	executorA.releaseClaim(ctx, "wf-locked")
	fenceToken, err = executorB.acquireClaim(ctx, "wf-locked")
	if err != nil {
		t.Fatalf("unexpected error after release: %v", err)
	}
	if fenceToken != 1 {
		t.Fatalf("got fence token %d, want 1 (fresh claim)", fenceToken)
	}
}

func TestRunWorkflow_ExpiredClaimTakeover(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	// Executor A claims with a very short lease (already expired on write).
	executorA := New(storage, Options{LeaseDuration: 1}) // 1 nanosecond
	fenceToken, err := executorA.acquireClaim(ctx, "wf-expire")
	if err != nil {
		t.Fatal(err)
	}
	if fenceToken != 1 {
		t.Fatalf("got fence token %d, want 1", fenceToken)
	}

	// Executor B should be able to take over the expired claim.
	executorB := New(storage, Options{})
	fenceToken, err = executorB.acquireClaim(ctx, "wf-expire")
	if err != nil {
		t.Fatalf("unexpected error taking over expired claim: %v", err)
	}
	if fenceToken != 2 {
		t.Fatalf("got fence token %d, want 2 (bumped)", fenceToken)
	}
}

func mustMarshalStepState(t *testing.T, name string, result any) []byte {
	t.Helper()
	resultJSON, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	state := StepState{
		Name:        name,
		Status:      StepCompleted,
		CompletedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Result:      resultJSON,
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestStep_WriteOnceFencing(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	// Simulate: another executor already wrote step "charge" for this workflow.
	key := executor.stepKey("wf-fence", "charge")
	if err := storage.Put(ctx, key, mustMarshalStepState(t, "charge", "ch_first_writer")); err != nil {
		t.Fatal(err)
	}

	result, err := RunWorkflow(ctx, executor, "wf-fence", func(wf *Workflow) (string, error) {
		// This step's result is already cached from the pre-write above.
		chargeID, err := Step(wf, "charge", func(ctx context.Context) (string, error) {
			// This should NOT execute because the step is already persisted.
			return "ch_second_writer", nil
		})
		if err != nil {
			return "", err
		}
		return chargeID, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Should get the first writer's result, not ours.
	if result != "ch_first_writer" {
		t.Fatalf("got %q, want %q (first writer should win)", result, "ch_first_writer")
	}
}

func TestStep_WriteOnceFencing_ConcurrentWrite(t *testing.T) {
	storage := NewMemoryStorage()
	executor := New(storage, Options{})
	ctx := context.Background()

	var executed atomic.Bool

	result, err := RunWorkflow(ctx, executor, "wf-fence2", func(wf *Workflow) (string, error) {
		// Simulate: we execute the step, but before our PutIfAbsent lands,
		// another executor already wrote the result. We do this by pre-writing
		// the step result between execution and persist.
		chargeID, err := Step(wf, "charge", func(ctx context.Context) (string, error) {
			executed.Store(true)

			// Sneak in a write from "another executor" before our PutIfAbsent.
			_ = storage.PutIfAbsent(ctx, executor.stepKey("wf-fence2", "charge"),
				mustMarshalStepState(t, "charge", "ch_other"))

			return "ch_mine", nil
		})
		if err != nil {
			return "", err
		}
		return chargeID, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !executed.Load() {
		t.Fatal("step function should have executed")
	}
	// The other executor's write happened first (inside our step fn, before
	// our PutIfAbsent), so their result should win.
	if result != "ch_other" {
		t.Fatalf("got %q, want %q (first writer should win via fencing)", result, "ch_other")
	}
}
