package durex

import (
	"context"
	"encoding/json"
	"errors"
	"path"
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
	return path.Join(e.opts.Prefix, "workflows", workflowID, "state.json")
}

func (e *Executor) stepKey(workflowID, stepName string) string {
	return path.Join(e.opts.Prefix, "workflows", workflowID, "steps", stepName+".json")
}

func (e *Executor) stepsPrefix(workflowID string) string {
	return path.Join(e.opts.Prefix, "workflows", workflowID, "steps") + "/"
}

func (e *Executor) loadWorkflowState(ctx context.Context, workflowID string) (*WorkflowState, error) {
	data, err := e.storage.Get(ctx, e.workflowKey(workflowID))
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

func (e *Executor) saveWorkflowState(ctx context.Context, state *WorkflowState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return e.storage.Put(ctx, e.workflowKey(state.ID), data)
}

func (e *Executor) loadAllStepResults(ctx context.Context, workflowID string) (map[string]json.RawMessage, error) {
	prefix := e.stepsPrefix(workflowID)
	keys, err := e.storage.List(ctx, prefix)
	if err != nil {
		return nil, err
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
