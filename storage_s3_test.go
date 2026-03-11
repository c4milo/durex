//go:build e2e

package durex

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

// testPrefix returns a unique prefix for this test run to avoid collisions.
func testPrefix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("test-%s/", hex.EncodeToString(b[:]))
}

// testBucket returns the bucket name from DUREX_TEST_BUCKET or defaults to "durex-test".
func testBucket() string {
	if b := os.Getenv("DUREX_TEST_BUCKET"); b != "" {
		return b
	}
	return "durex-test"
}

// newTestStorage creates an S3Storage connected to real S3.
// Set AWS_PROFILE env var to select the AWS SSO profile.
func newTestStorage(t *testing.T) (*S3Storage, string, func()) {
	t.Helper()

	ctx := context.Background()

	var opts []func(*config.LoadOptions) error
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}

	prefix := testPrefix(t)
	bucket := testBucket()

	storage := NewS3Storage(S3Config{
		Bucket:       bucket,
		Config:       cfg,
		UsePathStyle: false,
	})

	cleanup := func() {
		keys, err := storage.List(ctx, prefix)
		if err != nil {
			t.Logf("cleanup: listing objects under %q: %v", prefix, err)
			return
		}
		for _, key := range keys {
			if err := storage.Delete(ctx, key); err != nil {
				t.Logf("cleanup: deleting %q: %v", key, err)
			}
		}
	}

	return storage, prefix, cleanup
}

// newTestExecutor creates an Executor backed by real S3 with its own isolated prefix.
func newTestExecutor(t *testing.T) (*Executor, func()) {
	t.Helper()
	storage, prefix, cleanup := newTestStorage(t)
	executor := New(storage, Options{
		Prefix:        prefix + "durex/",
		LeaseDuration: 30 * time.Second,
	})
	return executor, cleanup
}

func TestS3Put(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "put-test"
	data := []byte("hello world")

	if err := storage.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after Put: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
}

func TestS3PutOverwrite(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "overwrite-test"

	if err := storage.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := storage.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get = %q, want %q", got, "v2")
	}
}

func TestS3GetNotFound(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	_, err := storage.Get(ctx, prefix+"nonexistent-key")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get nonexistent = %v, want ErrNotFound", err)
	}
}

func TestS3PutIfAbsent(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "putifabsent-test"

	if err := storage.PutIfAbsent(ctx, key, []byte("first")); err != nil {
		t.Fatalf("PutIfAbsent (first): %v", err)
	}

	err := storage.PutIfAbsent(ctx, key, []byte("second"))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("PutIfAbsent (second) = %v, want ErrAlreadyExists", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("first")) {
		t.Fatalf("Get = %q, want %q (write-once violated)", got, "first")
	}
}

func TestS3PutIfAbsentFencing(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "fencing-test"

	if err := storage.PutIfAbsent(ctx, key, []byte("winner")); err != nil {
		t.Fatalf("PutIfAbsent (winner): %v", err)
	}

	for i := range 3 {
		err := storage.PutIfAbsent(ctx, key, []byte(fmt.Sprintf("loser-%d", i)))
		if !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("PutIfAbsent (loser-%d) = %v, want ErrAlreadyExists", i, err)
		}
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("winner")) {
		t.Fatalf("Get = %q, want %q (fencing broken)", got, "winner")
	}
}

func TestS3List(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	keys := []string{
		prefix + "list/a",
		prefix + "list/b",
		prefix + "list/c",
		prefix + "other/x",
	}
	for _, k := range keys {
		if err := storage.Put(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	got, err := storage.List(ctx, prefix+"list/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)

	want := []string{prefix + "list/a", prefix + "list/b", prefix + "list/c"}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("List returned %d keys, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	all, err := storage.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("List all returned %d keys, want 4: %v", len(all), all)
	}
}

func TestS3ListEmpty(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	got, err := storage.List(ctx, prefix+"nothing/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List empty prefix returned %d keys, want 0", len(got))
	}
}

func TestS3Delete(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "delete-test"

	if err := storage.Put(ctx, key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := storage.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := storage.Get(ctx, key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestS3DeleteNonexistent(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	if err := storage.Delete(ctx, prefix+"does-not-exist"); err != nil {
		t.Fatalf("Delete nonexistent: %v", err)
	}
}

func TestS3LargeObject(t *testing.T) {
	storage, prefix, cleanup := newTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	key := prefix + "large-object"

	data := make([]byte, 1<<20)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	if err := storage.Put(ctx, key, data); err != nil {
		t.Fatalf("Put large object: %v", err)
	}

	got, err := storage.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get large object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("large object round-trip mismatch")
	}
}
