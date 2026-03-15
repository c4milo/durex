package durex

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound is returned when a key does not exist in storage.
	ErrNotFound = errors.New("durex: not found")

	// ErrAlreadyExists is returned by PutIfAbsent when the key already exists.
	// This is the fencing mechanism: S3 conditional writes reject duplicate writes,
	// ensuring that even if a lock holder is stale, it cannot overwrite another
	// holder's step result.
	ErrAlreadyExists = errors.New("durex: already exists")

	// ErrPreconditionFailed is returned by PutIfMatch when the object's current
	// ETag does not match the expected value. This means the object was modified
	// (or deleted) between the read and the conditional write.
	ErrPreconditionFailed = errors.New("durex: precondition failed")

	// ErrWorkflowAlreadyRunning is returned when attempting to run a workflow
	// that is currently claimed by another executor.
	ErrWorkflowAlreadyRunning = errors.New("durex: workflow already running")

	// ErrInvalidID is returned when a workflow ID or step name contains invalid characters.
	ErrInvalidID = errors.New("durex: invalid identifier")
)

// StepError wraps a step execution error with workflow and step context.
type StepError struct {
	WorkflowID string
	StepName   string
	Err        error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("durex: step %q in workflow %q failed: %v", e.StepName, e.WorkflowID, e.Err)
}

func (e *StepError) Unwrap() error { return e.Err }
