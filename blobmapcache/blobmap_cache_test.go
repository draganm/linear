package blobcache

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/blobmap"
	"github.com/stretchr/testify/require"
)

func TestBlobmapCache(t *testing.T) {

	// Create sample blobmap
	sampleTestDir := t.TempDir()
	sampleBlobmapPath := filepath.Join(sampleTestDir, "sample.blobmap")
	sampleBlobmapBuilder, err := blobmap.NewBuilder(sampleBlobmapPath, 0, 1)
	require.NoError(t, err)
	sampleBlobmapBuilder.Add(0, []byte("hello world"))
	err = sampleBlobmapBuilder.Build()
	require.NoError(t, err)

	// Create temp directory for test
	tempDir := t.TempDir()

	// Initialize cache with small size to test eviction
	cache, err := New(tempDir, 2048)
	require.NoError(t, err)

	// Test data
	testKey := "test-key"
	testData, err := os.ReadFile(sampleBlobmapPath)
	require.NoError(t, err)

	// Test writing and reading from cache
	err = cache.WithBlobmap(
		context.Background(),
		testKey,
		func(ctx context.Context, path string) error {
			return os.WriteFile(path, testData, 0644)
		},
		func(ctx context.Context, b *blobmap.Reader) error {
			// Verify the data was written correctly
			data, err := os.ReadFile(filepath.Join(tempDir, testKey))
			require.NoError(t, err)
			require.Equal(t, testData, data)
			return nil
		},
	)
	require.NoError(t, err)

	// Test cache hit
	err = cache.WithBlobmap(
		context.Background(),
		testKey,
		func(ctx context.Context, path string) error {
			// This shouldn't be called on cache hit
			t.Error("loadBlobMap called on cache hit")
			return nil
		},
		func(ctx context.Context, b *blobmap.Reader) error {
			// Verify the data is still correct
			data, err := os.ReadFile(filepath.Join(tempDir, testKey))
			require.NoError(t, err)
			require.Equal(t, testData, data)
			return nil
		},
	)
	require.NoError(t, err)

	// Test eviction by creating a new large entry
	largeKey := "large-key"
	largeData := make([]byte, 2048) // Larger than cache size

	err = cache.WithBlobmap(
		context.Background(),
		largeKey,
		func(ctx context.Context, path string) error {
			return os.WriteFile(path, largeData, 0644)
		},
		func(ctx context.Context, b *blobmap.Reader) error {
			return nil
		},
	)
	require.NoError(t, err)

	// Verify first entry was evicted
	_, err = os.Stat(filepath.Join(tempDir, testKey))
	require.True(t, os.IsNotExist(err), "first entry should have been evicted")
}
