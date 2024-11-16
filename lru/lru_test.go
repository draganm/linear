package lru

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Basic(t *testing.T) {
	removed := make(map[string]string)
	var removeMu sync.Mutex

	cache := NewCache[string](
		100,
		func(key string) (string, uint64, error) {
			return "value-" + key, 10, nil
		},
		func(key string, value string) {
			removeMu.Lock()
			removed[key] = value
			removeMu.Unlock()
		},
	)

	// Test single get
	value, err := cache.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, "value-key1", value)
	assert.Equal(t, uint64(10), cache.CurrentSize)

	// Test cache hit
	value, err = cache.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, "value-key1", value)
}

func TestCache_Eviction(t *testing.T) {
	removed := make(map[string]string)
	var removeMu sync.Mutex

	cache := NewCache[string](
		25,
		func(key string) (string, uint64, error) {
			return "value-" + key, 10, nil
		},
		func(key string, value string) {
			removeMu.Lock()
			removed[key] = value
			removeMu.Unlock()
		},
	)

	// Add items until eviction
	for i := 1; i <= 3; i++ {
		value, err := cache.Get(string(rune('0' + i)))
		require.NoError(t, err)
		assert.Equal(t, "value-"+string(rune('0'+i)), value)
	}

	removeMu.Lock()
	assert.Equal(t, map[string]string{"1": "value-1"}, removed)
	removeMu.Unlock()
}

func TestCache_LoadError(t *testing.T) {
	cache := NewCache[string](
		100,
		func(key string) (string, uint64, error) {
			return "", 0, errors.New("load error")
		},
		nil,
	)

	value, err := cache.Get("key1")
	assert.Error(t, err)
	assert.Equal(t, "", value)
}

func TestCache_SizeError(t *testing.T) {
	cache := NewCache[string](
		10,
		func(key string) (string, uint64, error) {
			return "value", 20, nil
		},
		nil,
	)

	_, err := cache.Get("key1")
	require.Error(t, err)
}

func TestCache_Parallel(t *testing.T) {
	const (
		goroutines = 100
		iterations = 1000
	)

	cache := NewCache[int](
		1000,
		func(key string) (int, uint64, error) {
			time.Sleep(time.Millisecond) // Simulate work
			return len(key), 1, nil
		},
		nil,
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := string(rune(j % 10))
				value, err := cache.Get(key)
				require.NoError(t, err)
				assert.Equal(t, 1, value)
			}
		}(i)
	}

	wg.Wait()
}

func TestCache_ParallelWithEviction(t *testing.T) {
	var (
		removeCount int
		removeMu    sync.Mutex
	)

	cache := NewCache[string](
		50,
		func(key string) (string, uint64, error) {
			time.Sleep(time.Millisecond)
			return key, 10, nil
		},
		func(key string, value string) {
			removeMu.Lock()
			removeCount++
			removeMu.Unlock()
		},
	)

	var wg sync.WaitGroup
	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := string(rune(j % 20))
				value, err := cache.Get(key)
				require.NoError(t, err)
				assert.Equal(t, key, value)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(10 * time.Millisecond) // Wait for eviction callbacks

	removeMu.Lock()
	assert.Greater(t, removeCount, 0)
	removeMu.Unlock()
}

func TestCache_EmptyCache(t *testing.T) {
	cache := NewCache[string](100, nil, nil)
	cache.removeOldest() // Should not panic
}

func TestCache_MoveToFront(t *testing.T) {
	cache := NewCache[string](
		100,
		func(key string) (string, uint64, error) {
			return key, 10, nil
		},
		nil,
	)

	// Add multiple items
	keys := []string{"1", "2", "3"}
	for _, key := range keys {
		value, err := cache.Get(key)
		require.NoError(t, err)
		assert.Equal(t, key, value)
	}

	// Access middle item
	value, err := cache.Get("2")
	require.NoError(t, err)
	assert.Equal(t, "2", value)

	// Verify order
	assert.Equal(t, "2", cache.ListHead.Key)
	assert.Equal(t, "3", cache.ListHead.Next.Key)
	assert.Equal(t, "1", cache.ListHead.Next.Next.Key)
}