//go:build e2e

package durex

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

// testPrefix returns a unique prefix for this test run to avoid collisions.
func testPrefix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("test-%s/", hex.EncodeToString(b[:]))
}

// testBucket returns the bucket name from DUREX_TEST_BUCKET or defaults to "durex-test".
func testBucket() string {
	if b := os.Getenv("DUREX_TEST_BUCKET"); b != "" {
		return b
	}
	return "durex-test"
}

// newTestStorage creates an S3Storage connected to real S3 using the sandbox-sre-AWSAdministratorAccess-472797112831 profile.
// It returns the storage, a cleanup function that removes all objects under the test prefix,
// and the prefix used.
func newTestStorage(t *testing.T) (*S3Storage, string, func()) {
	t.Helper()

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile("sandbox-sre-AWSAdministratorAccess-472797112831"),
	)
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}

	prefix := testPrefix(t)
	bucket := testBucket()

	storage := NewS3Storage(S3Config{
		Bucket:       bucket,
		Config:       cfg,
		UsePathStyle: false,
	})

	cleanup := func() {
		// List and delete all objects under the test prefix.
		keys, err := storage.List(ctx, prefix)
		if err != nil {
			t.Logf("cleanup: listing objects under %q: %v", prefix, err)
			return
		}
		for _, key := range keys {
			if err := storage.Delete(ctx, key); err != nil {
				t.Logf("cleanup: deleting %q: %v", key, err)
			}
		}
	}

	return storage, prefix, cleanup
}

func TestS3Put(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "put-test"
	data := []byte("hello world")

	if err := storage.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after Put: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
}

func TestS3PutOverwrite(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "overwrite-test"

	if err := storage.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := storage.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get = %q, want %q", got, "v2")
	}
}

func TestS3GetNotFound(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	_, err := storage.Get(ctx, prefix+"nonexistent-key")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get nonexistent = %v, want ErrNotFound", err)
	}
}

func TestS3PutIfAbsent(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "putifabsent-test"

	// First write should succeed.
	if err := storage.PutIfAbsent(ctx, key, []byte("first")); err != nil {
		t.Fatalf("PutIfAbsent (first): %v", err)
	}

	// Second write should return ErrAlreadyExists.
	err := storage.PutIfAbsent(ctx, key, []byte("second"))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("PutIfAbsent (second) = %v, want ErrAlreadyExists", err)
	}

	// Original value should be preserved.
	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("first")) {
		t.Fatalf("Get = %q, want %q (write-once violated)", got, "first")
	}
}

func TestS3PutIfAbsentFencing(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "fencing-test"

	// Simulate two concurrent writers. The first one wins.
	if err := storage.PutIfAbsent(ctx, key, []byte("winner")); err != nil {
		t.Fatalf("PutIfAbsent (winner): %v", err)
	}

	// Multiple subsequent attempts should all fail.
	for i := 0; i < 3; i++ {
		err := storage.PutIfAbsent(ctx, key, []byte(fmt.Sprintf("loser-%d", i)))
		if !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("PutIfAbsent (loser-%d) = %v, want ErrAlreadyExists", i, err)
		}
	}

	// Value remains the winner's.
	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("winner")) {
		t.Fatalf("Get = %q, want %q (fencing broken)", got, "winner")
	}
}

func TestS3List(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// Write several objects under the test prefix.
	keys := []string{
		prefix + "list/a",
		prefix + "list/b",
		prefix + "list/c",
		prefix + "other/x",
	}
	for _, k := range keys {
		if err := storage.Put(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	// List with "list/" sub-prefix should return only the three matching keys.
	got, err := storage.List(ctx, prefix+"list/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)

	want := []string{prefix + "list/a", prefix + "list/b", prefix + "list/c"}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("List returned %d keys, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// List with the full test prefix should return all four keys.
	all, err := storage.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("List all returned %d keys, want 4: %v", len(all), all)
	}
}

func TestS3ListEmpty(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	got, err := storage.List(ctx, prefix+"nothing/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List empty prefix returned %d keys, want 0", len(got))
	}
}

func TestS3Delete(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "delete-test"

	if err := storage.Put(ctx, key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := storage.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := storage.Get(ctx, key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestS3DeleteNonexistent(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	// Deleting a nonexistent key should not return an error.
	if err := storage.Delete(ctx, prefix+"does-not-exist"); err != nil {
		t.Fatalf("Delete nonexistent: %v", err)
	}
}

// newTestExecutor creates an Executor backed by real S3 with its own isolated prefix.
func newTestExecutor(t *testing.T) (*Executor, func()) {
	t.Helper()
	storage, prefix, cleanup := newTestStorage(t)
	executor := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 30 * time.Second,
	})
	return executor, cleanup
}

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
	for i := 0; i < 5; i++ {
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

	// Run a workflow whose step has a side effect counter.
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

	// Run a NEW workflow that tries to write the same step key via a different
	// workflow ID — this should be independent (different namespace).
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

	// Step should NOT have executed — the cached result from S3 should be used.
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
			executor.storage.PutIfAbsent(ctx, sneakKey, sneakData)

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
	// The other executor's write landed first, so their result wins.
	if result != "ch_other" {
		t.Fatalf("got %q, want %q (first writer wins via fencing)", result, "ch_other")
	}
}

func TestS3ConcurrentExecutors(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()

	// Two independent executors sharing the same S3 storage, simulating
	// two processes competing to run the same workflow.
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

	// Launch both concurrently.
	go run(executorA)
	go run(executorB)

	o1 := <-ch
	o2 := <-ch

	// Exactly one should succeed, the other should get ErrWorkflowAlreadyRunning
	// OR both succeed (if the first completes and releases before the second tries).
	// In either case, all successful results must be identical.
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

	// If both succeeded (sequential execution), results must match
	// because write-once step results ensure consistency.
	if len(successes) == 2 && successes[0] != successes[1] {
		t.Fatalf("both executors completed but results differ: %q vs %q", successes[0], successes[1])
	}

	// A third executor should get the cached result without re-executing.
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
		t.Fatal("third executor should have returned cached result, not re-executed")
	}
	if r3 != successes[0] {
		t.Fatalf("third executor got %q, want %q (cached result)", r3, successes[0])
	}
}

func TestS3ConcurrentExecutorsStepConsistency(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()

	// Executor A: claims, runs step-1, then we manually expire its claim
	// so executor B can take over — simulating a lease expiry scenario.
	executorA := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 1, // 1 nanosecond — expires immediately
	})

	// Run workflow with executor A: completes step-1 but "crashes" before step-2.
	var stepCount int
	_, err := RunWorkflow(ctx, executorA, "wf-consistency", func(wf *Workflow) (string, error) {
		_, err := Step(wf, "step-1", func(ctx context.Context) (string, error) {
			stepCount++
			return "A-wrote-this", nil
		})
		if err != nil {
			return "", err
		}
		// Simulate crash: return error so workflow is marked failed, not completed.
		return "", errors.New("simulated crash after step-1")
	})
	if err == nil {
		t.Fatal("expected error from simulated crash")
	}
	if stepCount != 1 {
		t.Fatalf("step-1 ran %d times, want 1", stepCount)
	}

	// Executor B takes over (A's lease is already expired).
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

	// Step-1 should NOT re-execute — executor A's result is in S3.
	if bStep1Called {
		t.Fatal("executor B should have used A's cached step-1, not re-executed")
	}

	// The result must use A's step-1 value, proving write-once consistency.
	if result != "A-wrote-this+step2" {
		t.Fatalf("got %q, want %q (A's step-1 result must be preserved)", result, "A-wrote-this+step2")
	}
}

func TestS3LargeObject(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "large-object"

	// 1 MB of random data.
	data := make([]byte, 1<<20)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	if err := storage.Put(ctx, key, data); err != nil {
		t.Fatalf("Put large object: %v", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get large object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("large object round-trip mismatch")
	}
}
