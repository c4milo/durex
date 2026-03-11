package durex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Step executes a named step within a workflow. If the step completed in a
// previous execution, its cached result is returned without calling fn.
//
// Step results are persisted with write-once semantics (S3 If-None-Match).
// This is the fencing guarantee: even if two executors race on the same step
// due to a stale lease, only the first writer's result is persisted. The
// second executor reads and adopts the winner's result rather than overwriting it.
//
// Step names must be unique within a workflow. The result type T must be
// JSON-serializable.
func Step[T any](wf *Workflow, name string, fn func(ctx context.Context) (T, error)) (result T, err error) {
	if err := validateID(name); err != nil {
		return result, fmt.Errorf("invalid step name: %w", err)
	}

	ctx, span := tracer.Start(wf.ctx, "durex.step",
		trace.WithAttributes(
			attribute.String("durex.workflow.id", wf.id),
			attribute.String("durex.step.name", name),
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

	// Check for cached result from a previous execution.
	wf.mu.Lock()
	cached, ok := wf.stepResults[name]
	wf.mu.Unlock()

	if ok {
		span.SetAttributes(attribute.Bool("durex.step.cached", true))
		if err := json.Unmarshal(cached, &result); err != nil {
			return result, fmt.Errorf("deserializing cached step %q result: %w", name, err)
		}
		return result, nil
	}

	// Check context before executing.
	if err := ctx.Err(); err != nil {
		return result, &StepError{WorkflowID: wf.id, StepName: name, Err: err}
	}

	// Apply step timeout if configured.
	if wf.executor.opts.StepTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wf.executor.opts.StepTimeout)
		defer cancel()
	}

	// Execute the step function. The span context propagates into fn,
	// so any outgoing HTTP calls or downstream spans are children of this step.
	execCtx, execSpan := tracer.Start(ctx, "durex.step.execute",
		trace.WithAttributes(attribute.String("durex.step.name", name)),
	)
	result, err = fn(execCtx)
	if err != nil {
		execSpan.RecordError(err)
		execSpan.SetStatus(codes.Error, err.Error())
		execSpan.End()
		return result, &StepError{WorkflowID: wf.id, StepName: name, Err: err}
	}
	execSpan.End()

	// Serialize the successful result.
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return result, fmt.Errorf("serializing step %q result: %w", name, err)
	}

	stepState := &StepState{
		Name:        name,
		Status:      StepCompleted,
		CompletedAt: time.Now().UTC(),
		Result:      resultJSON,
	}
	stepData, err := json.Marshal(stepState)
	if err != nil {
		return result, fmt.Errorf("serializing step %q state: %w", name, err)
	}

	// Write-once persist: the fencing guarantee.
	// If another executor already wrote this step, PutIfAbsent returns
	// ErrAlreadyExists. We read and adopt their result for consistency.
	_, persistSpan := tracer.Start(ctx, "durex.step.persist",
		trace.WithAttributes(attribute.String("durex.step.name", name)),
	)
	err = wf.executor.storage.PutIfAbsent(wf.ctx, wf.executor.stepKey(wf.id, name), stepData)
	if errors.Is(err, ErrAlreadyExists) {
		persistSpan.SetAttributes(attribute.Bool("durex.step.fenced", true))
		persistSpan.End()
		span.SetAttributes(attribute.Bool("durex.step.fenced", true))
		return readPersistedStepResult[T](wf, name)
	}
	if err != nil {
		persistSpan.RecordError(err)
		persistSpan.SetStatus(codes.Error, err.Error())
		persistSpan.End()
		return result, fmt.Errorf("persisting step %q result: %w", name, err)
	}
	persistSpan.End()

	// Cache for the current execution.
	wf.mu.Lock()
	wf.stepResults[name] = resultJSON
	wf.mu.Unlock()

	return result, nil
}

// readPersistedStepResult reads a step result that was written by another executor.
// This is the "adopt the winner" path in the fencing protocol.
func readPersistedStepResult[T any](wf *Workflow, name string) (result T, err error) {
	data, err := wf.executor.storage.Get(wf.ctx, wf.executor.stepKey(wf.id, name))
	if err != nil {
		return result, fmt.Errorf("reading fenced step %q result: %w", name, err)
	}

	var state StepState
	if err := json.Unmarshal(data, &state); err != nil {
		return result, fmt.Errorf("parsing fenced step %q state: %w", name, err)
	}

	if err := json.Unmarshal(state.Result, &result); err != nil {
		return result, fmt.Errorf("deserializing fenced step %q result: %w", name, err)
	}

	wf.mu.Lock()
	wf.stepResults[name] = state.Result
	wf.mu.Unlock()

	return result, nil
}
