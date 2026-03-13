//go:build e2e

package durex

import (
	"context"
	"errors"
	"testing"
)

// Failed workflow resumes from the failure point against real S3.
func TestS3FailedWorkflowResumption(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	shouldFail := true

	run := func() (string, error) {
		return RunWorkflow(ctx, executor, "wf-resume-fail", func(wf *Workflow) (string, error) {
			_, err := Step(wf, "step-a", func(ctx context.Context) (string, error) {
				return "a", nil
			})
			if err != nil {
				return "", err
			}
			if shouldFail {
				return "", errors.New("workflow-level failure")
			}
			return "success", nil
		})
	}

	// First run fails after step-a completes.
	_, err := run()
	if err == nil {
		t.Fatal("expected error")
	}

	// Workflow state should be failed (not completed).
	state, err := executor.loadWorkflowState(ctx, "wf-resume-fail")
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.Status != WorkflowFailed {
		t.Fatalf("got status %v, want WorkflowFailed", state)
	}

	// No result.json should exist for a failed workflow.
	resultKey := executor.workflowResultKey("wf-resume-fail")
	if _, err := executor.storage.Get(ctx, resultKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("result.json should not exist for failed workflow, got err: %v", err)
	}

	// Second run succeeds — step-a cached in S3, not re-executed.
	shouldFail = false
	result, err := run()
	if err != nil {
		t.Fatalf("resumed workflow should succeed: %v", err)
	}
	if result != "success" {
		t.Fatalf("got %q, want %q", result, "success")
	}
}

// Failed step leaves no object in S3.
func TestS3FailedStepNotPersisted(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	_, _ = RunWorkflow(ctx, executor, "wf-step-no-persist", func(wf *Workflow) (string, error) {
		_, err := Step(wf, "bad-step", func(ctx context.Context) (string, error) {
			return "", errors.New("step failure")
		})
		return "", err
	})

	// The failed step should have no object in S3.
	stepKey := executor.stepKey("wf-step-no-persist", "bad-step")
	if _, err := executor.storage.Get(ctx, stepKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed step should not be persisted, got err: %v", err)
	}
}

// Multi-step partial recovery against real S3: step-2 fails, retry skips
// step-1, retries step-2, then runs step-3.
func TestS3MultiStepPartialRecovery(t *testing.T) {
	executor, cleanup := newTestExecutor(t)
	defer cleanup()
	ctx := context.Background()

	var step1Count, step2Count, step3Count int
	shouldFailStep2 := true

	run := func() (string, error) {
		return RunWorkflow(ctx, executor, "wf-partial-3step", func(wf *Workflow) (string, error) {
			_, err := Step(wf, "step-1", func(ctx context.Context) (string, error) {
				step1Count++
				return "one", nil
			})
			if err != nil {
				return "", err
			}

			_, err = Step(wf, "step-2", func(ctx context.Context) (string, error) {
				step2Count++
				if shouldFailStep2 {
					return "", errors.New("transient")
				}
				return "two", nil
			})
			if err != nil {
				return "", err
			}

			_, err = Step(wf, "step-3", func(ctx context.Context) (string, error) {
				step3Count++
				return "three", nil
			})
			if err != nil {
				return "", err
			}

			return "all-done", nil
		})
	}

	// First run: step-1 OK, step-2 fails, step-3 never reached.
	_, err := run()
	if err == nil {
		t.Fatal("expected error")
	}
	if step1Count != 1 || step2Count != 1 || step3Count != 0 {
		t.Fatalf("counts after first run: step1=%d step2=%d step3=%d, want 1,1,0",
			step1Count, step2Count, step3Count)
	}

	// Verify step-1 is persisted in S3, step-2 is NOT.
	if _, err := executor.storage.Get(ctx, executor.stepKey("wf-partial-3step", "step-1")); err != nil {
		t.Fatalf("step-1 should be persisted: %v", err)
	}
	if _, err := executor.storage.Get(ctx, executor.stepKey("wf-partial-3step", "step-2")); !errors.Is(err, ErrNotFound) {
		t.Fatal("step-2 should NOT be persisted after failure")
	}

	// Second run: step-1 cached from S3, step-2 retries and succeeds, step-3 runs.
	shouldFailStep2 = false
	result, err := run()
	if err != nil {
		t.Fatalf("second run should succeed: %v", err)
	}
	if result != "all-done" {
		t.Fatalf("got %q, want %q", result, "all-done")
	}
	if step1Count != 1 {
		t.Fatalf("step-1 ran %d times, want 1 (cached from S3)", step1Count)
	}
	if step2Count != 2 {
		t.Fatalf("step-2 ran %d times, want 2", step2Count)
	}
	if step3Count != 1 {
		t.Fatalf("step-3 ran %d times, want 1", step3Count)
	}
}
