package cache

import (
	"sync"
	"tinycached/server/replacement"
	"tinycached/utils"
)

type Cache struct {
	mutex sync.Mutex
	cache replacement.Cache
}

func New(policy utils.PolicyType, maxBytes uint64) (obj *Cache) {
	obj = &Cache{}

	if obj.cache == nil {
		switch policy {
		case utils.PolicyLRU:
			obj.cache = replacement.NewLRUCache(maxBytes)
		case utils.PolicyLFU:
			obj.cache = replacement.NewLFUCache(maxBytes)
		}
	}

	return obj
}

func (obj *Cache) SetExpireTimeMs(key string, expireTimeMs int64) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	obj.cache.SetExpireTimeMs(key, expireTimeMs)
}

func (obj *Cache) Add(key string, value []byte) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	obj.cache.Add(key, value)
}

func (obj *Cache) Del(key string) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	obj.cache.Del(key)
}

func (obj *Cache) Get(key string) (value []byte, ok bool) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	value, ok = obj.cache.Get(key)
	return value, ok
}
