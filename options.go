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
}

func defaultOptions() Options {
	return Options{
		Prefix:        "durex/",
		LeaseDuration: 5 * time.Minute,
	}
}
