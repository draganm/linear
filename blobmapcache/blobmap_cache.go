package blobmapcache

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/draganm/blobmap"
	"github.com/draganm/linear/lru"
)

type BlobmapCache struct {
	cacheDir string
	cache    *lru.Cache[*syncedBlobmap]
}

type syncedBlobmap struct {
	blobmap *blobmap.Reader
	evicted bool
	mu      sync.RWMutex
}

func Open(cacheDir string, maxCacheSize uint64) (*BlobmapCache, error) {
	cache := &BlobmapCache{
		cacheDir: cacheDir,
		cache: lru.NewCache[*syncedBlobmap](maxCacheSize, func(key string, b *syncedBlobmap) {
			b.mu.Lock()
			b.evicted = true
			b.blobmap.Close()
			b.mu.Unlock()
			os.Remove(filepath.Join(cacheDir, key))
		}),
	}

	// Load existing blobs from cache directory
	entries, err := os.ReadDir(cacheDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("could not read cache directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		key := entry.Name()

		escapedKey := url.PathEscape(key)

		_, err = cache.cache.Get(escapedKey, func() (*syncedBlobmap, uint64, error) {
			blobmapPath := filepath.Join(cacheDir, escapedKey)
			st, err := os.Stat(blobmapPath)
			if err != nil {
				return nil, 0, fmt.Errorf("could not stat blobmap %s: %w", escapedKey, err)
			}

			b, err := blobmap.Open(blobmapPath)
			if err != nil {
				return nil, 0, fmt.Errorf("could not open blobmap %s: %w", escapedKey, err)
			}
			return &syncedBlobmap{blobmap: b, mu: sync.RWMutex{}}, uint64(st.Size()), nil
		})
		if err != nil {
			// Log error but continue loading other entries
			fmt.Printf("error loading cached blob %s: %v\n", key, err)
		}
	}

	return cache, nil
}

func (c *BlobmapCache) WithBlobmap(
	ctx context.Context,
	key string,
	loadBlobMap func(ctx context.Context, path string) error,
	fn func(ctx context.Context, b *blobmap.Reader) error,
) error {
	var b *syncedBlobmap
	escapedKey := url.PathEscape(key)
	for {
		var err error
		b, err = c.cache.Get(
			key,
			func() (*syncedBlobmap, uint64, error) {
				blobmapPath := filepath.Join(c.cacheDir, escapedKey)
				err := loadBlobMap(ctx, blobmapPath)
				if err != nil {
					return nil, 0, fmt.Errorf("could not load blobmap %s: %w", escapedKey, err)
				}

				st, err := os.Stat(blobmapPath)
				if err != nil {
					return nil, 0, fmt.Errorf("could not stat blobmap %s: %w", blobmapPath, err)
				}

				b, err := blobmap.Open(blobmapPath)
				if err != nil {
					return nil, 0, fmt.Errorf("could not open blobmap %s: %w", escapedKey, err)
				}
				return &syncedBlobmap{blobmap: b, mu: sync.RWMutex{}}, uint64(st.Size()), nil
			},
		)

		if err != nil {
			return fmt.Errorf("could not get blobmap %s: %w", escapedKey, err)
		}

		b.mu.RLock()
		if b.evicted {
			b.mu.RUnlock()
			continue
		}

		break
	}

	defer b.mu.RUnlock()

	return fn(ctx, b.blobmap)

}

func (c *BlobmapCache) Close() {
	c.cache.Close(func(s string, sb *syncedBlobmap) error {
		sb.mu.Lock()
		sb.blobmap.Close()
		sb.evicted = true
		sb.mu.Unlock()
		return nil
	})
}
