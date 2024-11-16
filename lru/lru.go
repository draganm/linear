package lru

import (
	"fmt"
	"sync"
)

type LinkedListEntry[T any] struct {
	Key   string
	Value T
	Size  uint64
	Prev  *LinkedListEntry[T]
	Next  *LinkedListEntry[T]
}

type Cache[T any] struct {
	ListHead    *LinkedListEntry[T]
	ListTail    *LinkedListEntry[T]
	Map         map[string]*LinkedListEntry[T]
	Loading     map[string]func() (T, error)
	MaxSize     uint64
	CurrentSize uint64
	mu          *sync.Mutex
	OnRemove    func(key string, value T)
}

func (c *Cache[T]) removeOldest() {
	if c.ListTail == nil {
		return
	}

	entry := c.ListTail
	delete(c.Map, entry.Key)

	c.ListTail = entry.Prev
	if c.ListTail != nil {
		c.ListTail.Next = nil
	} else {
		c.ListHead = nil
	}

	if c.OnRemove != nil {
		c.OnRemove(entry.Key, entry.Value)
	}

	c.CurrentSize -= entry.Size
}

func (c *Cache[T]) makeSpace(size uint64) error {
	if c.MaxSize < size {
		return fmt.Errorf("size %d is larger than cache size %d", size, c.MaxSize)
	}

	for c.CurrentSize+size > c.MaxSize {
		c.removeOldest()
	}
	return nil
}

func (c *Cache[T]) Get(key string, loadFn func() (T, uint64, error)) (T, error) {
	c.mu.Lock()

	entry, ok := c.Map[key]
	if ok {
		c.moveToFront(entry)
		c.mu.Unlock()
		return entry.Value, nil
	}

	loadOnce, exists := c.Loading[key]
	if exists {
		c.mu.Unlock()
		return loadOnce()
	}

	loadOnce = sync.OnceValues(func() (T, error) {
		c.mu.Unlock()

		value, size, err := loadFn()
		if err != nil {
			c.mu.Lock()
			delete(c.Loading, key)
			c.mu.Unlock()
			return value, err
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		err = c.makeSpace(size)
		if err != nil {
			delete(c.Loading, key)
			return value, err
		}

		entry := &LinkedListEntry[T]{
			Key:   key,
			Value: value,
			Size:  size,
		}
		c.Map[key] = entry
		c.addToFront(entry)
		c.CurrentSize += size
		delete(c.Loading, key)

		return value, nil
	})

	c.Loading[key] = loadOnce
	return loadOnce()
}

func (c *Cache[T]) moveToFront(entry *LinkedListEntry[T]) {
	if entry == c.ListHead {
		return
	}

	if entry.Prev != nil {
		entry.Prev.Next = entry.Next
	}
	if entry.Next != nil {
		entry.Next.Prev = entry.Prev
	}
	if entry == c.ListTail {
		c.ListTail = entry.Prev
	}

	entry.Prev = nil
	entry.Next = c.ListHead
	c.ListHead.Prev = entry
	c.ListHead = entry
}

func (c *Cache[T]) addToFront(entry *LinkedListEntry[T]) {
	if c.ListHead == nil {
		c.ListHead = entry
		c.ListTail = entry
		return
	}

	entry.Next = c.ListHead
	c.ListHead.Prev = entry
	c.ListHead = entry
}

func NewCache[T any](
	maxSize uint64,
	onRemove func(string, T),
) *Cache[T] {
	return &Cache[T]{
		Map:      make(map[string]*LinkedListEntry[T]),
		Loading:  make(map[string]func() (T, error)),
		MaxSize:  maxSize,
		mu:       &sync.Mutex{},
		OnRemove: onRemove,
	}
}
