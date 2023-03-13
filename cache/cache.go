package cache

import (
	"errors"
	"sync"
	"tinycached/replacement"
	"tinycached/utils"
)

type Getter struct {
}

func (g *Getter) Get(key string) ([]byte, error) {
	return []byte("Default getter"), nil
}

type cacheObj struct {
	mutex sync.Mutex
	cache replacement.Cache
}

func newCacheObj(policy utils.PolicyType, maxBytes uint64) (obj *cacheObj) {
	obj = &cacheObj{}

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

func (obj *cacheObj) setExpireTimeMs(key string, expireTimeMs int64) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	obj.cache.SetExpireTimeMs(key, expireTimeMs)
}

func (obj *cacheObj) add(key string, value []byte) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	obj.cache.Add(key, value)
}

func (obj *cacheObj) del(key string) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	obj.cache.Del(key)
}

func (obj *cacheObj) get(key string) (value []byte, ok bool) {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	value, ok = obj.cache.Get(key)
	return value, ok
}

type CacheGroup struct {
	name       string
	getter     Getter
	localCache *cacheObj
}

var groupsMap sync.Map

func newGroup(name string) *CacheGroup {
	g := &CacheGroup{
		name:       name,
		getter:     Getter{},
		localCache: newCacheObj(utils.PolicyLRU, 512),
	}
	groupsMap.Store(name, g)
	return g
}

func (g *CacheGroup) SetExpireTimeMs(key string, expireTimeMs int64) {
	g.localCache.setExpireTimeMs(key, expireTimeMs)
}

func (g *CacheGroup) Add(key string, value []byte) {
	g.localCache.add(key, value)
}

func (g *CacheGroup) Del(key string) {
	g.localCache.del(key)
}

func (g *CacheGroup) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, errors.New("empty key is not allowed")
	}

	// 先从本地缓存取
	if val, ok := g.getFromLocal(key); ok {
		return val, nil
	}

	// 然后从远程节点取
	if val, ok := g.getFromRemoteNode(key); ok {
		return val, nil
	}

	// 最后从用户调用取
	if val, err := g.getFromUserCallback(key); err != nil {
		return nil, err
	} else {
		return val, err
	}
}

func (g *CacheGroup) getFromLocal(key string) ([]byte, bool) {
	val, ok := g.localCache.get(key)
	if ok {
		return val, true
	} else {
		return nil, false
	}
}

func (g *CacheGroup) getFromRemoteNode(key string) ([]byte, bool) {
	return nil, false
}

func (g *CacheGroup) getFromUserCallback(key string) ([]byte, error) {
	if val, err := g.getter.Get(key); err != nil {
		return nil, err
	} else {
		g.localCache.add(key, val) // 加入本地缓存
		return val, nil
	}
}

func GetCacheGroup(name string) *CacheGroup {
	g, ok := groupsMap.Load(name)
	if !ok {
		g = newGroup(name)
	}
	return g.(*CacheGroup)
}
