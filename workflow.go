package durex

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Workflow holds the execution context for a running workflow.
// It is passed to the workflow function and used by [Step] to check and persist step results.
type Workflow struct {
	id         string
	executor   *Executor
	ctx        context.Context
	fenceToken int64

	mu          sync.Mutex
	stepResults map[string]json.RawMessage
}

// ID returns the workflow's unique identifier.
func (w *Workflow) ID() string { return w.id }

// Context returns the workflow's context.
func (w *Workflow) Context() context.Context { return w.ctx }

// RunWorkflow executes a durable workflow with the given ID. If the workflow has
// already completed in a previous execution, its cached result is returned without
// calling fn. If the workflow was partially executed (some steps completed), those
// steps are skipped on re-execution and their cached results are used.
//
// A distributed claim (lease) is acquired before execution to prevent concurrent
// runs of the same workflow ID. If another executor holds the claim,
// [ErrWorkflowAlreadyRunning] is returned. The claim is released when the
// workflow completes, fails, or panics.
//
// The workflow ID must be unique per logical operation. Using the same ID ensures
// exactly-once semantics across process restarts.
func RunWorkflow[T any](ctx context.Context, e *Executor, workflowID string, fn func(wf *Workflow) (T, error)) (result T, err error) {
	if err := validateID(workflowID); err != nil {
		return result, fmt.Errorf("invalid workflow ID: %w", err)
	}

	ctx, span := tracer.Start(ctx, "durex.workflow",
		trace.WithAttributes(
			attribute.String("durex.workflow.id", workflowID),
			attribute.String("durex.executor.owner", e.ownerID),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	// Fast path: if workflow already completed, return cached result.
	// No claim needed — the result is immutable once written.
	existing, err := e.loadWorkflowState(ctx, workflowID)
	if err != nil {
		return result, fmt.Errorf("loading workflow state: %w", err)
	}
	if existing != nil && existing.Status == WorkflowCompleted {
		span.SetAttributes(attribute.Bool("durex.workflow.cached", true))
		if err := json.Unmarshal(existing.Result, &result); err != nil {
			return result, fmt.Errorf("deserializing cached workflow result: %w", err)
		}
		return result, nil
	}

	// Acquire the distributed claim.
	fenceToken, err := e.acquireClaim(ctx, workflowID)
	if err != nil {
		return result, err
	}
	defer e.releaseClaim(ctx, workflowID)

	span.SetAttributes(attribute.Int64("durex.workflow.fence_token", fenceToken))

	// Load any previously completed step results for recovery.
	stepResults, err := e.loadAllStepResults(ctx, workflowID)
	if err != nil {
		return result, fmt.Errorf("loading step results: %w", err)
	}

	span.SetAttributes(attribute.Int("durex.workflow.recovered_steps", len(stepResults)))

	// Mark workflow as running.
	now := time.Now().UTC()
	wfState := &WorkflowState{
		ID:        workflowID,
		Status:    WorkflowRunning,
		StartedAt: now,
	}
	if existing != nil {
		wfState.StartedAt = existing.StartedAt
	}
	if err := e.saveWorkflowState(ctx, wfState); err != nil {
		return result, fmt.Errorf("saving workflow state: %w", err)
	}

	wf := &Workflow{
		id:          workflowID,
		executor:    e,
		ctx:         ctx,
		fenceToken:  fenceToken,
		stepResults: stepResults,
	}

	// Execute the workflow function, catching panics.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("durex: workflow %q panicked: %v", workflowID, r)
			span.SetAttributes(attribute.Bool("durex.workflow.panicked", true))
			completedAt := time.Now().UTC()
			wfState.Status = WorkflowFailed
			wfState.CompletedAt = &completedAt
			wfState.Error = err.Error()
			_ = e.saveWorkflowState(ctx, wfState)
		}
	}()

	result, err = fn(wf)

	completedAt := time.Now().UTC()
	wfState.CompletedAt = &completedAt

	if err != nil {
		wfState.Status = WorkflowFailed
		wfState.Error = err.Error()
		span.SetAttributes(attribute.String("durex.workflow.status", string(WorkflowFailed)))
		if saveErr := e.saveWorkflowState(ctx, wfState); saveErr != nil {
			return result, fmt.Errorf("saving failed workflow state: %w (original error: %w)", saveErr, err)
		}
		return result, err
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return result, fmt.Errorf("serializing workflow result: %w", err)
	}

	wfState.Status = WorkflowCompleted
	wfState.Result = resultJSON
	span.SetAttributes(attribute.String("durex.workflow.status", string(WorkflowCompleted)))
	if err := e.saveWorkflowState(ctx, wfState); err != nil {
		return result, fmt.Errorf("saving completed workflow state: %w", err)
	}

	return result, nil
}
