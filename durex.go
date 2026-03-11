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
	return &Executor{
		storage: storage,
		opts:    opts,
		ownerID: newOwnerID(),
	}
}

// validateID checks that an identifier is safe for use as a storage key component.
func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: must not be empty", ErrInvalidID)
	}
	if strings.ContainsAny(id, "/\\") {
		return fmt.Errorf("%w: must not contain path separators", ErrInvalidID)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("%w: must not be '.' or '..'", ErrInvalidID)
	}
	return nil
}

func newOwnerID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("durex: failed to generate owner ID: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
