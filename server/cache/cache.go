package cache

import (
	"sync"
)

type Cache struct {
	mutex sync.Mutex
	cache *LRU
}

func New(maxBytes uint64) (c *Cache) {
	c = &Cache{cache: NewLRUCache(maxBytes)}
	return c
}

func (c *Cache) SetExpireTimeMs(key string, expireTimeMs int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache.SetExpireTimeMs(key, expireTimeMs)
}

func (c *Cache) Add(key string, value []byte) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache.Add(key, value)
}

func (c *Cache) Del(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache.Del(key)
}

func (c *Cache) Get(key string) (value []byte, ok bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	value, ok = c.cache.Get(key)
	return value, ok
}
