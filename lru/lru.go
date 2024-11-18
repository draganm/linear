package lru

import (
	"errors"
	"fmt"
	"sync"
)

type LinkedListEntry[T any] struct {
	key   string
	value T
	size  uint64
	prev  *LinkedListEntry[T]
	next  *LinkedListEntry[T]
}

type Cache[T any] struct {
	listHead    *LinkedListEntry[T]
	listTail    *LinkedListEntry[T]
	mapByKey    map[string]*LinkedListEntry[T]
	loading     map[string]func() (T, error)
	maxSize     uint64
	currentSize uint64
	mu          *sync.Mutex
	onRemove    func(key string, value T)
}

func (c *Cache[T]) removeOldest() {
	if c.listTail == nil {
		return
	}

	entry := c.listTail
	delete(c.mapByKey, entry.key)

	c.listTail = entry.prev
	if c.listTail != nil {
		c.listTail.next = nil
	} else {
		c.listHead = nil
	}

	if c.onRemove != nil {
		c.onRemove(entry.key, entry.value)
	}

	c.currentSize -= entry.size
}

func (c *Cache[T]) makeSpace(size uint64) error {
	if c.maxSize < size {
		return fmt.Errorf("size %d is larger than cache size %d", size, c.maxSize)
	}

	for c.currentSize+size > c.maxSize {
		c.removeOldest()
	}
	return nil
}

func (c *Cache[T]) Get(key string, loadFn func() (T, uint64, error)) (T, error) {
	c.mu.Lock()

	entry, ok := c.mapByKey[key]
	if ok {
		c.moveToFront(entry)
		c.mu.Unlock()
		return entry.value, nil
	}

	loadOnce, exists := c.loading[key]
	if exists {
		c.mu.Unlock()
		return loadOnce()
	}

	loadOnce = sync.OnceValues(func() (T, error) {
		c.mu.Unlock()

		value, size, err := loadFn()
		if err != nil {
			c.mu.Lock()
			delete(c.loading, key)
			c.mu.Unlock()
			return value, err
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		err = c.makeSpace(size)
		if err != nil {
			delete(c.loading, key)
			return value, err
		}

		entry := &LinkedListEntry[T]{
			key:   key,
			value: value,
			size:  size,
		}
		c.mapByKey[key] = entry
		c.addToFront(entry)
		c.currentSize += size
		delete(c.loading, key)

		return value, nil
	})

	c.loading[key] = loadOnce
	return loadOnce()
}

func (c *Cache[T]) moveToFront(entry *LinkedListEntry[T]) {
	if entry == c.listHead {
		return
	}

	if entry.prev != nil {
		entry.prev.next = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	}
	if entry == c.listTail {
		c.listTail = entry.prev
	}

	entry.prev = nil
	entry.next = c.listHead
	c.listHead.prev = entry
	c.listHead = entry
}

func (c *Cache[T]) addToFront(entry *LinkedListEntry[T]) {
	if c.listHead == nil {
		c.listHead = entry
		c.listTail = entry
		return
	}

	entry.next = c.listHead
	c.listHead.prev = entry
	c.listHead = entry
}

func NewCache[T any](
	maxSize uint64,
	onRemove func(string, T),
) *Cache[T] {
	return &Cache[T]{
		mapByKey: make(map[string]*LinkedListEntry[T]),
		loading:  make(map[string]func() (T, error)),
		maxSize:  maxSize,
		mu:       &sync.Mutex{},
		onRemove: onRemove,
	}
}

func (c *Cache[T]) Close(closeValue func(string, T) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for key, entry := range c.mapByKey {
		err = errors.Join(err, closeValue(key, entry.value))
		if err != nil {
			return err
		}
	}
	return nil
}
