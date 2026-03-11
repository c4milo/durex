package durex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WorkflowState is the persisted state of a workflow execution.
type WorkflowState struct {
	ID          string          `json:"id"`
	Status      WorkflowStatus  `json:"status"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
}

// StepState is the persisted state of a step execution.
type StepState struct {
	Name        string          `json:"name"`
	Status      StepStatus      `json:"status"`
	CompletedAt time.Time       `json:"completed_at"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
}

func (e *Executor) workflowKey(workflowID string) string {
	return e.opts.Prefix + "workflows/" + workflowID + "/state.json"
}

// workflowResultKey returns the key for the immutable workflow result.
// This is separate from state.json so that PutIfAbsent provides write-once
// semantics for the completed result while state.json remains mutable.
func (e *Executor) workflowResultKey(workflowID string) string {
	return e.opts.Prefix + "workflows/" + workflowID + "/result.json"
}

func (e *Executor) stepKey(workflowID, stepName string) string {
	return e.opts.Prefix + "workflows/" + workflowID + "/steps/" + stepName + ".json"
}

func (e *Executor) stepsPrefix(workflowID string) string {
	return e.opts.Prefix + "workflows/" + workflowID + "/steps/"
}

func (e *Executor) loadWorkflowState(ctx context.Context, workflowID string) (*WorkflowState, error) {
	// Check the immutable result key first — if it exists, the workflow
	// is definitively completed (write-once guarantee).
	data, err := e.storage.Get(ctx, e.workflowResultKey(workflowID))
	if err == nil {
		var state WorkflowState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		return &state, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	// Fall back to mutable state (running/failed).
	data, err = e.storage.Get(ctx, e.workflowKey(workflowID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var state WorkflowState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// saveWorkflowState persists mutable workflow state (running, failed).
// These are informational and may be overwritten on retry.
func (e *Executor) saveWorkflowState(ctx context.Context, state *WorkflowState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return e.storage.Put(ctx, e.workflowKey(state.ID), data)
}

// completeWorkflow persists the final workflow result with write-once semantics
// to a dedicated result key (separate from mutable state.json). If another
// executor already wrote the result, its value is returned instead.
// This mirrors the step fencing pattern: first writer wins.
func completeWorkflow[T any](ctx context.Context, e *Executor, state *WorkflowState) (T, bool, error) {
	var zero T

	data, err := json.Marshal(state)
	if err != nil {
		return zero, false, err
	}

	err = e.storage.PutIfAbsent(ctx, e.workflowResultKey(state.ID), data)
	if err == nil {
		// Also update the mutable state for observability (best-effort).
		_ = e.saveWorkflowState(ctx, state)
		return zero, false, nil // we won the write
	}
	if !errors.Is(err, ErrAlreadyExists) {
		return zero, false, err
	}

	// Another executor completed first. Read and adopt their result.
	winnerData, err := e.storage.Get(ctx, e.workflowResultKey(state.ID))
	if err != nil {
		return zero, false, fmt.Errorf("reading fenced workflow result: %w", err)
	}

	var winner WorkflowState
	if err := json.Unmarshal(winnerData, &winner); err != nil {
		return zero, false, fmt.Errorf("parsing fenced workflow result: %w", err)
	}

	var result T
	if err := json.Unmarshal(winner.Result, &result); err != nil {
		return zero, false, fmt.Errorf("deserializing fenced workflow result: %w", err)
	}
	return result, true, nil
}

func (e *Executor) loadAllStepResults(ctx context.Context, workflowID string) (map[string]json.RawMessage, error) {
	prefix := e.stepsPrefix(workflowID)
	keys, err := e.storage.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	if len(keys) > e.opts.MaxSteps {
		return nil, fmt.Errorf("workflow %q has %d steps, exceeding limit of %d",
			workflowID, len(keys), e.opts.MaxSteps)
	}

	results := make(map[string]json.RawMessage, len(keys))
	for _, key := range keys {
		data, err := e.storage.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		var state StepState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}

		if state.Status == StepCompleted {
			name := strings.TrimPrefix(key, prefix)
			name = strings.TrimSuffix(name, ".json")
			results[name] = state.Result
		}
	}
	return results, nil
}
