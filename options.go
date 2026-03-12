package durex

import (
	"context"
	"encoding/json"
	"time"
)

// Hooks provides lifecycle callbacks for workflow and step events.
// All hooks are optional and best-effort — a hook failure does not affect
// the workflow's durable state. Hooks fire after the durable write succeeds,
// so the S3 source of truth is always consistent regardless of hook outcome.
//
// The context passed to hooks carries the active OpenTelemetry span, so any
// work done inside a hook (database writes, notifications) appears as a child
// span in the trace.
type Hooks struct {
	// OnWorkflowStart is called after the workflow claim is acquired and
	// before the workflow function executes.
	OnWorkflowStart func(ctx context.Context, id string)

	// OnWorkflowComplete is called after the workflow result is durably
	// persisted to S3. The result is the JSON-encoded return value.
	OnWorkflowComplete func(ctx context.Context, id string, result json.RawMessage)

	// OnWorkflowFailed is called after a failed workflow state is persisted.
	OnWorkflowFailed func(ctx context.Context, id string, err error)

	// OnStepComplete is called after a step result is durably persisted.
	// It is NOT called for cached steps that were loaded from a previous run.
	OnStepComplete func(ctx context.Context, workflowID, stepName string, result json.RawMessage)
}

// Options configures the durable executor.
type Options struct {
	// Prefix is the key prefix for all objects stored by durex.
	// Default: "durex/"
	Prefix string

	// StepTimeout is the default timeout for individual step execution.
	// Zero means no timeout.
	// Default: 0
	StepTimeout time.Duration

	// LeaseDuration controls how long a workflow claim is valid before it expires.
	// If a holder crashes without releasing the claim, another executor can take
	// over after this duration. Set this longer than your longest expected workflow.
	// Default: 5 minutes
	LeaseDuration time.Duration

	// MaxSteps limits the number of step results loaded during recovery.
	// Prevents unbounded memory allocation if the bucket contains a large number
	// of step objects (either by design or from a malicious write).
	// Default: 1000
	MaxSteps int

	// MaxIDLength is the maximum length of workflow IDs and step names.
	// Prevents unbounded S3 key lengths.
	// Default: 256
	MaxIDLength int

	// Hooks provides lifecycle callbacks for building indexes, sending
	// notifications, or integrating with external systems. All hooks are
	// optional and best-effort.
	Hooks Hooks
}

func defaultOptions() Options {
	return Options{
		Prefix:        "durex/",
		LeaseDuration: 5 * time.Minute,
		MaxSteps:      1000,
		MaxIDLength:   256,
	}
}
