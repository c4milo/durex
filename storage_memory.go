package durex

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryStorage is an in-memory ObjectStorage implementation intended for testing.
type MemoryStorage struct {
	mu      sync.RWMutex
	data    map[string][]byte
	etags   map[string]string
	counter uint64
}

// NewMemoryStorage returns a new in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data:  make(map[string][]byte),
		etags: make(map[string]string),
	}
}

func (m *MemoryStorage) nextETag() string {
	m.counter++
	return fmt.Sprintf(`"etag-%d"`, m.counter)
}

func (m *MemoryStorage) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	m.etags[key] = m.nextETag()
	return nil
}

func (m *MemoryStorage) PutIfAbsent(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		return ErrAlreadyExists
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	m.etags[key] = m.nextETag()
	return nil
}

func (m *MemoryStorage) GetWithETag(_ context.Context, key string) ([]byte, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, m.etags[key], nil
}

func (m *MemoryStorage) PutIfMatch(_ context.Context, key string, data []byte, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.etags[key]
	if !ok || current != etag {
		return ErrPreconditionFailed
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	m.etags[key] = m.nextETag()
	return nil
}

func (m *MemoryStorage) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *MemoryStorage) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *MemoryStorage) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	delete(m.etags, key)
	return nil
}
