//go:build e2e

package durex

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestS3WorkflowRecovery(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	shouldFail := true
	var stepACount, stepBCount int

	run := func() (string, error) {
		return RunWorkflow(ctx, executor, "wf-recovery", func(wf *Workflow) (string, error) {
			_, err := Step(wf, "step-a", func(ctx context.Context) (string, error) {
				stepACount++
				return "a-done", nil
			})
			if err != nil {
				return "", err
			}

			result, err := Step(wf, "step-b", func(ctx context.Context) (string, error) {
				stepBCount++
				if shouldFail {
					return "", errors.New("transient")
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
		t.Fatal("expected error from step-b")
	}
	if stepACount != 1 {
		t.Fatalf("step-a ran %d times, want 1", stepACount)
	}
	if stepBCount != 1 {
		t.Fatalf("step-b ran %d times, want 1", stepBCount)
	}

	// Second run: step-a should be skipped (cached in S3), step-b retries.
	shouldFail = false
	result, err := run()
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if result != "b-done" {
		t.Fatalf("got %q, want %q", result, "b-done")
	}
	if stepACount != 1 {
		t.Fatalf("step-a ran %d times after recovery, want 1 (should be cached)", stepACount)
	}
	if stepBCount != 2 {
		t.Fatalf("step-b ran %d times, want 2", stepBCount)
	}
}

func TestS3WorkflowIdempotency(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	var callCount int

	run := func() (string, error) {
		return RunWorkflow(ctx, executor, "wf-idempotent", func(wf *Workflow) (string, error) {
			callCount++
			v, err := Step(wf, "compute", func(ctx context.Context) (string, error) {
				return "result-1", nil
			})
			if err != nil {
				return "", err
			}
			return v, nil
		})
	}

	// First call executes.
	r1, err := run()
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if r1 != "result-1" {
		t.Fatalf("got %q, want %q", r1, "result-1")
	}
	if callCount != 1 {
		t.Fatalf("workflow fn called %d times, want 1", callCount)
	}

	// Subsequent calls return cached result without re-executing.
	for i := range 5 {
		r, err := run()
		if err != nil {
			t.Fatalf("run %d: %v", i+2, err)
		}
		if r != r1 {
			t.Fatalf("run %d: got %q, want %q", i+2, r, r1)
		}
	}
	if callCount != 1 {
		t.Fatalf("workflow fn called %d times after 5 reruns, want 1", callCount)
	}
}

func TestS3StepIdempotency(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	var stepCount int

	result, err := RunWorkflow(ctx, executor, "wf-step-idemp", func(wf *Workflow) (string, error) {
		v, err := Step(wf, "side-effect", func(ctx context.Context) (string, error) {
			stepCount++
			return "effect-applied", nil
		})
		if err != nil {
			return "", err
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "effect-applied" {
		t.Fatalf("got %q, want %q", result, "effect-applied")
	}
	if stepCount != 1 {
		t.Fatalf("step ran %d times, want 1", stepCount)
	}

	// Different workflow ID with same step name — independent namespace.
	var step2Count int
	result2, err := RunWorkflow(ctx, executor, "wf-step-idemp-2", func(wf *Workflow) (string, error) {
		v, err := Step(wf, "side-effect", func(ctx context.Context) (string, error) {
			step2Count++
			return "different-effect", nil
		})
		if err != nil {
			return "", err
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result2 != "different-effect" {
		t.Fatalf("got %q, want %q", result2, "different-effect")
	}
	if step2Count != 1 {
		t.Fatalf("step ran %d times, want 1", step2Count)
	}
}

func TestS3WriteOnceFencing(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	// Pre-write a step result as if another executor wrote it first.
	stepKey := executor.stepKey("wf-fencing", "charge")
	firstResult := mustMarshalStepState(t, "charge", "ch_first_writer")
	if err := executor.storage.PutIfAbsent(ctx, stepKey, firstResult); err != nil {
		t.Fatalf("pre-writing step: %v", err)
	}

	var executed bool
	result, err := RunWorkflow(ctx, executor, "wf-fencing", func(wf *Workflow) (string, error) {
		v, err := Step(wf, "charge", func(ctx context.Context) (string, error) {
			executed = true
			return "ch_second_writer", nil
		})
		if err != nil {
			return "", err
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if executed {
		t.Fatal("step fn should not have executed (result was pre-cached in S3)")
	}
	if result != "ch_first_writer" {
		t.Fatalf("got %q, want %q (first writer should win)", result, "ch_first_writer")
	}
}

func TestS3ConcurrentWriteOnceFencing(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	var executed bool
	result, err := RunWorkflow(ctx, executor, "wf-fence-race", func(wf *Workflow) (string, error) {
		v, err := Step(wf, "charge", func(ctx context.Context) (string, error) {
			executed = true

			// Simulate another executor writing the step result between our
			// execution and our PutIfAbsent.
			sneakKey := executor.stepKey("wf-fence-race", "charge")
			sneakData := mustMarshalStepState(t, "charge", "ch_other")
			_ = executor.storage.PutIfAbsent(ctx, sneakKey, sneakData)

			return "ch_mine", nil
		})
		if err != nil {
			return "", err
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !executed {
		t.Fatal("step fn should have executed")
	}
	if result != "ch_other" {
		t.Fatalf("got %q, want %q (first writer wins via fencing)", result, "ch_other")
	}
}

func TestS3ConcurrentExecutors(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()

	executorA := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 30 * time.Second,
	})
	executorB := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 30 * time.Second,
	})

	type outcome struct {
		result string
		err    error
	}

	ch := make(chan outcome, 2)

	run := func(e *Executor) {
		r, err := RunWorkflow(ctx, e, "wf-concurrent", func(wf *Workflow) (string, error) {
			v, err := Step(wf, "step1", func(ctx context.Context) (string, error) {
				return "computed-by-" + e.ownerID[:8], nil
			})
			if err != nil {
				return "", err
			}
			return v, nil
		})
		ch <- outcome{r, err}
	}

	go run(executorA)
	go run(executorB)

	o1 := <-ch
	o2 := <-ch

	var successes []string
	for _, o := range []outcome{o1, o2} {
		if o.err != nil {
			if !errors.Is(o.err, ErrWorkflowAlreadyRunning) {
				t.Fatalf("unexpected error: %v", o.err)
			}
			continue
		}
		successes = append(successes, o.result)
	}

	if len(successes) == 0 {
		t.Fatal("both executors failed, expected at least one to succeed")
	}
	if len(successes) == 2 && successes[0] != successes[1] {
		t.Fatalf("both completed but results differ: %q vs %q", successes[0], successes[1])
	}

	// A third executor should get the cached result.
	executorC := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 30 * time.Second,
	})
	var called bool
	r3, err := RunWorkflow(ctx, executorC, "wf-concurrent", func(wf *Workflow) (string, error) {
		called = true
		return "should-not-run", nil
	})
	if err != nil {
		t.Fatalf("third executor: %v", err)
	}
	if called {
		t.Fatal("third executor should have returned cached result")
	}
	if r3 != successes[0] {
		t.Fatalf("third executor got %q, want %q", r3, successes[0])
	}
}

func TestS3ConcurrentExecutorsStepConsistency(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()

	executorA := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 1, // 1 nanosecond — expires immediately
	})

	var stepCount int
	_, err := RunWorkflow(ctx, executorA, "wf-consistency", func(wf *Workflow) (string, error) {
		_, err := Step(wf, "step-1", func(ctx context.Context) (string, error) {
			stepCount++
			return "A-wrote-this", nil
		})
		if err != nil {
			return "", err
		}
		return "", errors.New("simulated crash after step-1")
	})
	if err == nil {
		t.Fatal("expected error from simulated crash")
	}
	if stepCount != 1 {
		t.Fatalf("step-1 ran %d times, want 1", stepCount)
	}

	executorB := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 30 * time.Second,
	})

	var bStep1Called bool
	result, err := RunWorkflow(ctx, executorB, "wf-consistency", func(wf *Workflow) (string, error) {
		v1, err := Step(wf, "step-1", func(ctx context.Context) (string, error) {
			bStep1Called = true
			return "B-wrote-this", nil
		})
		if err != nil {
			return "", err
		}

		v2, err := Step(wf, "step-2", func(ctx context.Context) (string, error) {
			return v1 + "+step2", nil
		})
		if err != nil {
			return "", err
		}
		return v2, nil
	})
	if err != nil {
		t.Fatalf("executor B: %v", err)
	}

	if bStep1Called {
		t.Fatal("executor B should have used A's cached step-1")
	}
	if result != "A-wrote-this+step2" {
		t.Fatalf("got %q, want %q (A's step-1 must be preserved)", result, "A-wrote-this+step2")
	}
}
