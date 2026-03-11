package durex

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("github.com/c4milo/durex")

// Executor is the entry point for running durable workflows.
type Executor struct {
	storage ObjectStorage
	opts    Options
	ownerID string
}

// New creates a new durable executor backed by the given storage.
// Each executor is assigned a unique owner ID used for claim tracking.
func New(storage ObjectStorage, opts Options) *Executor {
	defaults := defaultOptions()
	if opts.Prefix == "" {
		opts.Prefix = defaults.Prefix
	}
	if !strings.HasSuffix(opts.Prefix, "/") {
		opts.Prefix += "/"
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = defaults.LeaseDuration
	}
	if opts.MaxSteps == 0 {
		opts.MaxSteps = defaults.MaxSteps
	}
	if opts.MaxIDLength == 0 {
		opts.MaxIDLength = defaults.MaxIDLength
	}
	return &Executor{
		storage: storage,
		opts:    opts,
		ownerID: newOwnerID(),
	}
}

// validateID checks that an identifier is safe for use as a storage key
// component. Only alphanumeric characters, hyphens, underscores, and dots
// are allowed. This prevents log injection via control characters, S3 key
// issues from special characters, and unbounded key lengths.
func (e *Executor) validateID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: must not be empty", ErrInvalidID)
	}
	if len(id) > e.opts.MaxIDLength {
		return fmt.Errorf("%w: must not exceed %d characters", ErrInvalidID, e.opts.MaxIDLength)
	}
	for _, c := range id {
		if !isIDChar(c) {
			return fmt.Errorf("%w: contains invalid character %q (allowed: a-z A-Z 0-9 . _ -)", ErrInvalidID, c)
		}
	}
	return nil
}

func isIDChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'
}

func newOwnerID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("durex: failed to generate owner ID: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
