package durex

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStorage_PutGet(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()

	if err := s.Put(ctx, "key1", []byte("value1")); err != nil {
		t.Fatal(err)
	}

	data, err := s.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "value1" {
		t.Fatalf("got %q, want %q", data, "value1")
	}
}

func TestMemoryStorage_GetNotFound(t *testing.T) {
	s := NewMemoryStorage()
	_, err := s.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestMemoryStorage_PutIfAbsent(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()

	// First write succeeds.
	if err := s.PutIfAbsent(ctx, "key1", []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// Second write returns ErrAlreadyExists.
	err := s.PutIfAbsent(ctx, "key1", []byte("v2"))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("got %v, want ErrAlreadyExists", err)
	}

	// Original value is preserved.
	data, _ := s.Get(ctx, "key1")
	if string(data) != "v1" {
		t.Fatalf("got %q, want %q", data, "v1")
	}
}

func TestMemoryStorage_List(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()

	if err := s.Put(ctx, "prefix/a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "prefix/b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "other/c", []byte("3")); err != nil {
		t.Fatal(err)
	}

	keys, err := s.List(ctx, "prefix/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want 2", len(keys))
	}
}

func TestMemoryStorage_Delete(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()

	if err := s.Put(ctx, "key1", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "key1"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(ctx, "key1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatal("expected key to be deleted")
	}

	// Delete non-existent key should not error.
	if err := s.Delete(ctx, "missing"); err != nil {
		t.Fatalf("unexpected error deleting missing key: %v", err)
	}
}

func TestMemoryStorage_PutOverwrites(t *testing.T) {
	s := NewMemoryStorage()
	ctx := context.Background()

	if err := s.Put(ctx, "key1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "key1", []byte("v2")); err != nil {
		t.Fatal(err)
	}

	data, _ := s.Get(ctx, "key1")
	if string(data) != "v2" {
		t.Fatalf("got %q, want %q", data, "v2")
	}
}
