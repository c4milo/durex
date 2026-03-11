package durex

import "time"

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
}

func defaultOptions() Options {
	return Options{
		Prefix:        "durex/",
		LeaseDuration: 5 * time.Minute,
		MaxSteps:      1000,
		MaxIDLength:   256,
	}
}
