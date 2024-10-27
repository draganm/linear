package archive

import (
	"sync"

	"github.com/draganm/blobmap"
)

type CacheManager struct {
	mu             *sync.RWMutex
	cacheSizeLimit uint64
	cachedBlobmaps map[string]*blobmap.Reader

	lru []string
}

func NewCacheManager(cacheSizeLimit uint64) *CacheManager {
	return &CacheManager{
		mu:             &sync.RWMutex{},
		cachedBlobmaps: make(map[string]*blobmap.Reader),
		cacheSizeLimit: cacheSizeLimit,
	}
}
