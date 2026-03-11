package durex

import "context"

// ObjectStorage is the interface for durable state persistence.
// Implementations must be safe for concurrent use.
type ObjectStorage interface {
	// Put stores data at the given key, overwriting any existing data.
	Put(ctx context.Context, key string, data []byte) error

	// PutIfAbsent stores data at the given key only if the key does not already exist.
	// Returns ErrAlreadyExists if the key exists. On S3 this uses If-None-Match
	// conditional writes, providing the fencing guarantee described by Kleppmann:
	// the storage itself rejects stale writes regardless of lock state.
	PutIfAbsent(ctx context.Context, key string, data []byte) error

	// Get retrieves data at the given key.
	// Returns ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// List returns all keys matching the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the given key. It is not an error if the key does not exist.
	Delete(ctx context.Context, key string) error
}
