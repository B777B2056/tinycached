package replacement

import (
	"container/list"
	"time"
	"tinycached/server/persistence"
	"tinycached/utils"
	"unsafe"
)

type Cache interface {
	Add(key string, value []byte)
	Get(key string) ([]byte, bool)
	Del(key string)
	SetExpireTimeMs(key string, expireTimeMs int64)
}

type cacheValue struct {
	value        []byte
	expireTimeMs int64
	bornTimeMs   int64
}

type kvPair struct {
	key    string
	cvalue cacheValue
}

type LRUCache struct {
	usedBytes  uint64
	maxBytes   uint64
	cacheQueue *list.List
	cacheMap   map[string]*list.Element
	aof        *persistence.Aof
}

func NewLRUCache(maxBytes uint64) (lru *LRUCache) {
	lru = &LRUCache{
		maxBytes:   maxBytes,
		cacheQueue: list.New(),
		cacheMap:   make(map[string]*list.Element),
		aof:        persistence.AofInstance(),
	}
	return lru
}

func (lru *LRUCache) SetExpireTimeMs(key string, expireTimeMs int64) {
	elem, ok := lru.cacheMap[key]
	if !ok {
		return
	}
	elem.Value.(*kvPair).cvalue.expireTimeMs = expireTimeMs
}

func (lru *LRUCache) Add(key string, value []byte) {
	// 如果内存耗尽，则淘汰缓存项，直至有内存空间
	newCache := &kvPair{
		key: key,
		cvalue: cacheValue{
			value:        utils.CopyBytes(value),
			expireTimeMs: 0,
			bornTimeMs:   time.Now().UnixMilli(),
		},
	}
	curBytes := uint64(unsafe.Sizeof(newCache))
	for lru.usedBytes+curBytes > lru.maxBytes {
		node := lru.cacheQueue.Back()
		elem := node.Value.(*kvPair)
		// 更新已使用字节数
		elemBytes := uint64(unsafe.Sizeof(elem))
		lru.usedBytes -= elemBytes
		// 淘汰缓存
		lru.aof.Append([]byte("DEL"), []byte(elem.key))
		delete(lru.cacheMap, elem.key)
		lru.cacheQueue.Remove(node)
	}
	// 更新缓存
	if _, ok := lru.Get(key); ok {
		// 查看待插入的key是否已存在；若存在则提升到队头，并修改其值
		oldElem := lru.cacheQueue.Front().Value.(*kvPair)
		lru.cacheQueue.Front().Value = newCache
		// 更新已使用字节数
		lru.usedBytes += uint64(len(value) - len(oldElem.cvalue.value))
		return
	} else {
		// 插入新的缓存项
		lru.cacheQueue.PushFront(newCache)
		lru.cacheMap[key] = lru.cacheQueue.Front()
		// 更新已使用字节数
		lru.usedBytes += curBytes
	}
}

func (lru *LRUCache) Del(key string) {
	elem, ok := lru.cacheMap[key]
	if !ok {
		return
	}
	// 更新已使用字节数
	elemBytes := uint64(unsafe.Sizeof(elem))
	lru.usedBytes -= elemBytes
	// 删除目标缓存
	delete(lru.cacheMap, key)
	lru.cacheQueue.Remove(elem)
}

func (lru *LRUCache) Get(key string) ([]byte, bool) {
	elem, ok := lru.cacheMap[key]
	if !ok {
		return nil, false
	}
	target := elem.Value.(*kvPair).cvalue.value
	targetCopy := utils.CopyBytes(target)
	// 检查过期时间，若过期则销毁
	bornTime := elem.Value.(*kvPair).cvalue.bornTimeMs
	expireTime := elem.Value.(*kvPair).cvalue.expireTimeMs
	if expireTime > 0 && bornTime+expireTime >= time.Now().UnixMilli() {
		// 更新已使用字节数
		elemBytes := uint64(unsafe.Sizeof(elem.Value.(*kvPair)))
		lru.usedBytes -= elemBytes
		// 淘汰缓存
		lru.aof.Append([]byte("DEL"), []byte(key))
		delete(lru.cacheMap, key)
		lru.cacheQueue.Remove(elem)
	} else {
		// 更新目标缓存的位置
		lru.cacheQueue.MoveToFront(elem)
		lru.cacheMap[key] = lru.cacheQueue.Front()
	}
	return targetCopy, true
}

type LFUCache struct {
}

func NewLFUCache(maxBytes uint64) (lfu *LFUCache) {
	return nil
}

func (lfu *LFUCache) SetExpireTimeMs(key string, expireTimeMs int64) {

}

func (lfu *LFUCache) Add(key string, value []byte) {

}

func (lfu *LFUCache) Del(key string) {

}

func (lfu *LFUCache) Get(key string) ([]byte, bool) {
	return nil, false
}
